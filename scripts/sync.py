import os
import sys
import time
import hashlib
import requests
import json
import logging
import traceback
import concurrent.futures
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

# --- Configuration Environment Variables --- #
LOKI_URL = os.getenv("LOKI_URL")
LOKI_USER = os.getenv("LOKI_USER")
LOKI_API_KEY = os.getenv("LOKI_API_KEY")
LOKI_QUERY = os.getenv("LOKI_QUERY")

SCARF_API_TOKEN = os.getenv("SCARF_API_TOKEN")
SCARF_ENTITY_ID = os.getenv("SCARF_ENTITY_ID")
ORGANIZATION_NAME = os.getenv("ORGANIZATION_NAME", "OpenVSX")
SCARF_BATCH_SIZE = int(os.getenv("SCARF_BATCH_SIZE", "500"))
SCARF_MAX_WORKERS = int(os.getenv("SCARF_MAX_WORKERS", "10"))

# Must match the CronJob's schedule interval (charts/values.yaml: sync.intervalMinutes).
# Only used as the window size for the very first run, before any checkpoint exists.
SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "5"))

# Caps how far back a resumed run will query if the checkpoint is stale (e.g. after an
# outage or several skipped ticks), so a long gap doesn't turn into one huge Loki query.
MAX_LOOKBACK_MINUTES = int(os.getenv("MAX_LOOKBACK_MINUTES", "60"))

# --- Checkpoint state, persisted in a ConfigMap via the in-cluster Kubernetes API --- #
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
CHECKPOINT_CONFIGMAP_NAME = os.getenv("CHECKPOINT_CONFIGMAP_NAME", "scarf-sync-checkpoint")
CHECKPOINT_KEY = "last_synced_until_ns"


def _k8s_request(method, path, **kwargs):
    with open(f"{SA_DIR}/token") as f:
        token = f.read().strip()
    with open(f"{SA_DIR}/namespace") as f:
        namespace = f.read().strip()
    host = os.environ["KUBERNETES_SERVICE_HOST"]
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    url = f"https://{host}:{port}/api/v1/namespaces/{namespace}{path}"
    return requests.request(
        method,
        url,
        headers={"Authorization": f"Bearer {token}", **kwargs.pop("headers", {})},
        verify=f"{SA_DIR}/ca.crt",
        timeout=10,
        **kwargs,
    )


def load_checkpoint():
    """Returns the end-of-window timestamp (ns) of the last successful run, or None
    if there isn't one yet (first run) or it can't be read (e.g. running outside a
    cluster) -- in both cases the caller falls back to the fixed interval window."""
    try:
        resp = _k8s_request("GET", f"/configmaps/{CHECKPOINT_CONFIGMAP_NAME}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        raw = resp.json().get("data", {}).get(CHECKPOINT_KEY)
        return int(raw) if raw else None
    except Exception as e:
        print(f"Warning: could not load sync checkpoint, falling back to default window: {e}")
        return None


def save_checkpoint(end_ns):
    """Persists end_ns as the new high-water mark. Best-effort: if this fails, the
    next run simply re-queries the same window again (safe, since Scarf events are
    deduped by $unique_id)."""
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": CHECKPOINT_CONFIGMAP_NAME},
        "data": {
            CHECKPOINT_KEY: str(end_ns),
            "last_synced_until_iso": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(end_ns / 1e9)),
        },
    }
    try:
        resp = _k8s_request(
            "PATCH",
            f"/configmaps/{CHECKPOINT_CONFIGMAP_NAME}",
            json=body,
            headers={"Content-Type": "application/merge-patch+json"},
        )
        if resp.status_code == 404:
            resp = _k8s_request("POST", "/configmaps", json=body)
        resp.raise_for_status()
    except Exception as e:
        print(f"Warning: failed to persist sync checkpoint, next run will re-query this window: {e}")


def determine_window():
    now_ns = int(time.time() * 1e9)
    checkpoint_ns = load_checkpoint()

    if checkpoint_ns is None:
        print(f"No checkpoint found; using default {SYNC_INTERVAL_MINUTES}-minute window.")
        return now_ns - SYNC_INTERVAL_MINUTES * 60 * int(1e9), now_ns

    max_lookback_ns = MAX_LOOKBACK_MINUTES * 60 * int(1e9)
    if now_ns - checkpoint_ns > max_lookback_ns:
        skipped_minutes = (now_ns - checkpoint_ns - max_lookback_ns) / 1e9 / 60
        print(
            f"Checkpoint is older than MAX_LOOKBACK_MINUTES={MAX_LOOKBACK_MINUTES}; "
            f"clamping window and permanently skipping ~{skipped_minutes:.1f} minutes of logs."
        )
        return now_ns - max_lookback_ns, now_ns

    return checkpoint_ns, now_ns


LOKI_PAGE_LIMIT = 5000


def fetch_loki_logs(start_ns, end_ns):
    print(f"Initiating log pull from Grafana Loki ({(end_ns - start_ns) / 1e9 / 60:.1f} minute window)...")

    endpoint = f"{LOKI_URL}/loki/api/v1/query_range"
    auth = HTTPBasicAuth(LOKI_USER, LOKI_API_KEY) if LOKI_USER else None

    combined_results = []
    seen_entries = set()
    cursor_start = start_ns
    page = 1

    while True:
        params = {
            "query": LOKI_QUERY,
            "start": cursor_start,
            "end": end_ns,
            "limit": LOKI_PAGE_LIMIT,
            "direction": "forward",
        }

        try:
            response = requests.get(endpoint, params=params, auth=auth, timeout=60)
            response.raise_for_status()
            payload = response.json()
        except Exception as e:
            print(f"Error fetching logs from Loki: {e}")
            sys.exit(1)

        results = payload.get("data", {}).get("result", [])
        entry_count = 0
        max_ts_ns = cursor_start

        for stream in results:
            new_values = []
            for ts_str, log_line in stream.get("values", []):
                entry_count += 1
                ts_ns = int(ts_str)
                max_ts_ns = max(max_ts_ns, ts_ns)
                # Re-querying from the previous page's max timestamp re-fetches
                # everything tied to that exact nanosecond, so drop repeats.
                key = (ts_ns, log_line)
                if key in seen_entries:
                    continue
                seen_entries.add(key)
                new_values.append([ts_str, log_line])
            if new_values:
                combined_results.append({"stream": stream.get("stream", {}), "values": new_values})

        print(f"Loki page {page}: fetched {entry_count} entries (start={cursor_start}).")

        if entry_count < LOKI_PAGE_LIMIT:
            break

        if max_ts_ns <= cursor_start:
            # More than LOKI_PAGE_LIMIT entries share the exact same nanosecond
            # timestamp, so the cursor can't advance -- bail out instead of looping
            # forever. Pathological and effectively never hit in practice.
            print("Warning: Loki page did not advance; stopping pagination early.")
            break

        cursor_start = max_ts_ns
        page += 1

    return {"data": {"result": combined_results}}

def parse_telemetry(loki_data):
    events = []
    results = loki_data.get("data", {}).get("result", [])
    
    for result in results:
        for values in result.get("values", []):
            # Loki format: [timestamp_ns, log_line_string]
            timestamp_ns, log_line = values
            
            try:
                # Convert timestamp_ns to ISO 8601 string for Scarf's $time field
                timestamp_seconds = int(timestamp_ns) / 1e9
                iso_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(timestamp_seconds))
                
                # Parse log string matching your exact schema layout
                log_json = json.loads(log_line)
                
                # Build Scarf Event Payload aligning with Scarf V2 specifications
                # Special variables require a "$" prefix to activate Scarf's IP/UA enrichment engines
                # $type classifies the event for Scarf's dashboards; $unique_id makes re-sent
                # log lines (overlapping Loki windows, retries) override rather than duplicate
                url = log_json.get("url") or ""
                event = {
                    "$type": "download" if "download" in url.lower() else "request",
                    "$unique_id": hashlib.sha256(log_line.encode("utf-8")).hexdigest(),
                    "$remote_address": log_json.get("client_ip"),
                    "$user_agent": log_json.get("request_user_agent"),
                    "$time": iso_time,
                    "url": url or None,
                    "request_method": log_json.get("request_method"),
                    "status": log_json.get("status"),
                    "status_message": log_json.get("status_message"),
                    "state": log_json.get("state")
                }
                
                # Filter out lines that miss essential traffic data
                if event["$remote_address"] or event["$user_agent"]:
                    events.append(event)
                    
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                # Log the full traceback if you need deep stack info for structural bugs
                logger.debug(f"Parsing traceback: {traceback.format_exc()}")
                
                # Still cleanly continue the runtime loop
                continue
                
    return events

def ship_to_scarf(events):
    if not events:
        print("No telemetry events parsed in this window. Exiting cleanly.")
        return True

    print(f"Parsed {len(events)} telemetry events. Initializing authenticated Scarf batch export...")
    headers = {
        "Authorization": f"Bearer {SCARF_API_TOKEN}",
        "Content-Type": "application/x-ndjson"
    }

    # The standard structural format required by Scarf's ingestion API
    url = f"https://api.scarf.sh/v2/packages/{ORGANIZATION_NAME}/{SCARF_ENTITY_ID}/import"

    batches = [events[i:i + SCARF_BATCH_SIZE] for i in range(0, len(events), SCARF_BATCH_SIZE)]

    # A plain requests.post() per call opens a fresh TCP+TLS connection to Scarf every
    # time; a shared Session with a pool sized to the worker count lets threads reuse
    # keep-alive connections instead of re-handshaking on every one of these batches.
    session = requests.Session()
    session.mount("https://", requests.adapters.HTTPAdapter(
        pool_connections=SCARF_MAX_WORKERS, pool_maxsize=SCARF_MAX_WORKERS
    ))

    def send_batch(batch_num, batch):
        # Scarf's import API expects newline-delimited JSON: one event object
        # per line, NOT a single JSON document wrapping them in an array/object
        ndjson_body = "\n".join(json.dumps(event) for event in batch)
        try:
            res = session.post(url, data=ndjson_body.encode("utf-8"), headers=headers, timeout=30)
            res.raise_for_status()
            # HTTP 200 only means Scarf accepted the request, not that every event
            # ingested cleanly -- check the response body for per-event rejections
            try:
                body = res.json()
            except ValueError:
                body = res.text
            return batch_num, len(batch), body, None
        except Exception as e:
            return batch_num, len(batch), None, e

    all_batches_ok = True
    with session, concurrent.futures.ThreadPoolExecutor(max_workers=SCARF_MAX_WORKERS) as executor:
        futures = [executor.submit(send_batch, i + 1, batch) for i, batch in enumerate(batches)]
        # Process completions as they arrive; continue draining the rest even
        # if some batches fail, to isolate processing failures per batch
        for future in concurrent.futures.as_completed(futures):
            batch_num, batch_len, body, error = future.result()
            if error is not None:
                print(f"Error shipping batch {batch_num} to Scarf: {error}")
                all_batches_ok = False
            else:
                print(f"Batch {batch_num}: Sent {batch_len} records. Response: {body}")

    return all_batches_ok


def validate_config():
    # Catches missing required config up front -- otherwise a missing SCARF_ENTITY_ID,
    # for example, doesn't fail loudly; it just builds a broken "None" import URL.
    required = {
        "LOKI_URL": LOKI_URL,
        "LOKI_QUERY": LOKI_QUERY,
        "SCARF_API_TOKEN": SCARF_API_TOKEN,
        "SCARF_ENTITY_ID": SCARF_ENTITY_ID,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        print(f"Error: missing required environment variable(s): {', '.join(missing)}")
        sys.exit(1)

if __name__ == "__main__":
    run_started_at = time.monotonic()
    try:
        validate_config()
        window_start_ns, window_end_ns = determine_window()

        stage_started_at = time.monotonic()
        loki_payload = fetch_loki_logs(window_start_ns, window_end_ns)
        print(f"Fetch stage took {time.monotonic() - stage_started_at:.1f}s.")

        stage_started_at = time.monotonic()
        telemetry_events = parse_telemetry(loki_payload)
        print(f"Parse stage took {time.monotonic() - stage_started_at:.1f}s.")

        stage_started_at = time.monotonic()
        shipped_ok = ship_to_scarf(telemetry_events)
        print(f"Ship stage took {time.monotonic() - stage_started_at:.1f}s.")

        if shipped_ok:
            # Only advance the checkpoint once everything shipped -- if this run got cut
            # short or a batch failed, the next run re-covers the same window instead of
            # silently losing it (safe: Scarf dedupes on $unique_id).
            save_checkpoint(window_end_ns)
        else:
            print("One or more batches failed to ship; checkpoint left unchanged so this window is retried.")
            sys.exit(1)
    finally:
        # Runs even on sys.exit() from validate_config/fetch_loki_logs, so every
        # run -- successful or not -- reports how long it took.
        print(f"Run finished in {time.monotonic() - run_started_at:.1f}s.")

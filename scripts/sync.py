import os
import sys
import time
import hashlib
import requests
import json
import logging
import traceback
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

# --- Configuration Environment Variables --- #
LOKI_URL = os.getenv("LOKI_URL", "https://logs-prod-018.grafana.net")
LOKI_USER = os.getenv("LOKI_USER")
LOKI_API_KEY = os.getenv("LOKI_API_KEY")
LOKI_QUERY = os.getenv("LOKI_QUERY", '{service_name="fastly_cdn", env="production"}')

SCARF_API_TOKEN = os.getenv("SCARF_API_TOKEN")
SCARF_ENTITY_ID = os.getenv("SCARF_ENTITY_ID")
ORGANIZATION_NAME = os.getenv("ORGANIZATION_NAME", "OpenVSX")
SCARF_BATCH_SIZE = int(os.getenv("SCARF_BATCH_SIZE", "500"))

# Must match the CronJob's schedule interval (charts/values.yaml: sync.intervalMinutes).
# Only used as the window size for the very first run, before any checkpoint exists.
SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "15"))

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


def fetch_loki_logs(start_ns, end_ns):
    print(f"Initiating log pull from Grafana Loki ({(end_ns - start_ns) / 1e9 / 60:.1f} minute window)...")

    endpoint = f"{LOKI_URL}/loki/api/v1/query_range"
    params = {
        "query": LOKI_QUERY,
        "start": start_ns,
        "end": end_ns,
        "limit": 5000
    }

    try:
        response = requests.get(
            endpoint, 
            params=params, 
            auth=HTTPBasicAuth(LOKI_USER, LOKI_API_KEY) if LOKI_USER else None,
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching logs from Loki: {e}")
        sys.exit(1)

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
                event = {
                    "$type": "download",
                    "$unique_id": hashlib.sha256(log_line.encode("utf-8")).hexdigest(),
                    "$remote_address": log_json.get("client_ip"),
                    "$user_agent": log_json.get("request_user_agent"),
                    "$time": iso_time,
                    "url": log_json.get("url"),
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

    all_batches_ok = True

    # Memory-bounded batch array division
    for i in range(0, len(events), SCARF_BATCH_SIZE):
        batch = events[i:i + SCARF_BATCH_SIZE]
        # Scarf's import API expects newline-delimited JSON: one event object
        # per line, NOT a single JSON document wrapping them in an array/object
        ndjson_body = "\n".join(json.dumps(event) for event in batch)

        try:
            res = requests.post(url, data=ndjson_body.encode("utf-8"), headers=headers, timeout=30)
            res.raise_for_status()
            # HTTP 200 only means Scarf accepted the request, not that every event
            # ingested cleanly -- check the response body for per-event rejections
            try:
                body = res.json()
            except ValueError:
                body = res.text
            print(f"Batch {i // SCARF_BATCH_SIZE + 1}: Sent {len(batch)} records. Response: {body}")
        except Exception as e:
            print(f"Error shipping batch to Scarf: {e}")
            # Continue trying subsequent batches to isolate processing failures
            all_batches_ok = False

    return all_batches_ok

if __name__ == "__main__":
    window_start_ns, window_end_ns = determine_window()
    loki_payload = fetch_loki_logs(window_start_ns, window_end_ns)
    telemetry_events = parse_telemetry(loki_payload)

    if ship_to_scarf(telemetry_events):
        # Only advance the checkpoint once everything shipped -- if this run got cut
        # short or a batch failed, the next run re-covers the same window instead of
        # silently losing it (safe: Scarf dedupes on $unique_id).
        save_checkpoint(window_end_ns)
    else:
        print("One or more batches failed to ship; checkpoint left unchanged so this window is retried.")
        sys.exit(1)

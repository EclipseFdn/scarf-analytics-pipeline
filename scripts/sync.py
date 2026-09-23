import os
import sys
import time
import hashlib
import random
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

# Transient Scarf failures (timeouts, connection drops, 429, 5xx) are retried per batch
# with exponential backoff, so one slow moment doesn't force re-shipping the whole window.
# Retrying a request that timed out after Scarf received it is safe: Scarf dedupes on $unique_id.
SCARF_MAX_ATTEMPTS = 4
SCARF_BACKOFF_BASE_SECONDS = 2
# (connect, read): fail fast if Scarf is unreachable, but give a slow-but-alive import more room.
SCARF_TIMEOUT = (5, 60)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Must match the CronJob's schedule interval (charts/values.yaml: sync.intervalMinutes).
# Each run syncs consecutive windows of this many minutes, starting from the checkpoint,
# until it reaches "now", so a backlog (outage, failed runs) is worked off within a
# single run while every Loki query and Scarf export stays window-sized.
SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "5"))

# --- Checkpoint state, persisted in a ConfigMap via the in-cluster Kubernetes API --- #
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
CHECKPOINT_CONFIGMAP_NAME = os.getenv("CHECKPOINT_CONFIGMAP_NAME", "scarf-sync-checkpoint")
CHECKPOINT_KEY = "last_synced_until_ns"

# --- Failure alerting via a Slack incoming webhook (unset = alerting disabled) --- #
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
# Alert once the job has failed this many runs in a row; 1 = alert on the first failure.
ALERT_AFTER_CONSECUTIVE_FAILURES = int(os.getenv("ALERT_AFTER_CONSECUTIVE_FAILURES", "1"))
# Alert when the checkpoint is more than this far behind real time and still losing
# ground, i.e. windows take longer to sync than the time they cover.
LAG_ALERT_MINUTES = int(os.getenv("LAG_ALERT_MINUTES", "60"))
DEPLOY_ENVIRONMENT = os.getenv("DEPLOY_ENVIRONMENT", "unknown")
# Stored alongside the checkpoint so an outage alerts once, not on every retry run.
FAILURE_COUNT_KEY = "consecutive_failures"
ALERT_SENT_KEY = "failure_alert_sent"
LAG_ALERT_SENT_KEY = "lag_alert_sent"


class SyncError(Exception):
    """An expected, already-explained failure: the message goes to the logs and the alert
    as-is, without a traceback."""


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


def load_state():
    """Returns the checkpoint ConfigMap's data ({} if it doesn't exist yet, i.e. first
    run), or None if it can't be read (e.g. running outside a cluster)."""
    try:
        resp = _k8s_request("GET", f"/configmaps/{CHECKPOINT_CONFIGMAP_NAME}")
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json().get("data") or {}
    except Exception as e:
        print(f"Warning: could not load sync state ConfigMap: {e}")
        return None


def _patch_state(data):
    """Merge-patches data into the checkpoint ConfigMap (creating it if missing); keys
    not in data are left untouched."""
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": CHECKPOINT_CONFIGMAP_NAME},
        "data": data,
    }
    resp = _k8s_request(
        "PATCH",
        f"/configmaps/{CHECKPOINT_CONFIGMAP_NAME}",
        json=body,
        headers={"Content-Type": "application/merge-patch+json"},
    )
    if resp.status_code == 404:
        resp = _k8s_request("POST", "/configmaps", json=body)
    resp.raise_for_status()


def _iso(ns):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(ns / 1e9))


def save_checkpoint(end_ns):
    """Persists end_ns as the new high-water mark and clears the failure streak.
    Best-effort: if this fails, the next run simply re-queries the same window again
    (safe, since Scarf events are deduped by $unique_id)."""
    try:
        _patch_state({
            CHECKPOINT_KEY: str(end_ns),
            "last_synced_until_iso": _iso(end_ns),
            FAILURE_COUNT_KEY: "0",
            ALERT_SENT_KEY: "false",
        })
    except Exception as e:
        print(f"Warning: failed to persist sync checkpoint, next run will re-query this window: {e}")


def notify_slack(text):
    """Posts text to the Slack webhook. Returns whether it was delivered; never raises,
    so a broken webhook can't mask the job's own result."""
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set; skipping Slack alert.")
        return False
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"Warning: failed to send Slack alert: {e}")
        return False


def record_failure(state, error, window):
    """Bumps the consecutive-failure count and alerts once per outage: when the streak
    reaches ALERT_AFTER_CONSECUTIVE_FAILURES, not again on every retry run after that.
    If the state couldn't be read the streak is unknown, so alert anyway -- a possibly
    duplicate alert beats a silent outage."""
    failures = None if state is None else int(state.get(FAILURE_COUNT_KEY) or 0) + 1
    already_alerted = state is not None and state.get(ALERT_SENT_KEY) == "true"

    alert_sent = already_alerted
    if not already_alerted and (failures is None or failures >= ALERT_AFTER_CONSECUTIVE_FAILURES):
        window_text = f"{_iso(window[0])} to {_iso(window[1])}" if window else "not determined"
        error_text = str(error)[:1500]
        alert_sent = notify_slack(
            f":rotating_light: *Scarf export failing* ({DEPLOY_ENVIRONMENT})\n"
            f"*Pod:* `{os.getenv('HOSTNAME', 'unknown')}`\n"
            f"*Consecutive failed runs:* {failures if failures is not None else 'unknown'}\n"
            f"*Window:* {window_text}\n"
            f"*Error:* ```{error_text}```\n"
            "No further alerts will be sent until a run succeeds."
        )

    if state is not None:
        try:
            _patch_state({FAILURE_COUNT_KEY: str(failures), ALERT_SENT_KEY: "true" if alert_sent else "false"})
        except Exception as e:
            print(f"Warning: failed to persist failure count: {e}")


def check_lag(state, lag_ns, losing_ground, caught_up):
    """Alerts once when the job is more than LAG_ALERT_MINUTES behind and not catching up,
    and once more when it has caught up again. A job that is behind but gaining ground
    (e.g. working off a backlog after an outage) doesn't alert. Returns the new value of
    the lag-alert-sent flag."""
    lag_alert_sent = (state or {}).get(LAG_ALERT_SENT_KEY) == "true"
    lag_minutes = lag_ns / 60e9

    if not lag_alert_sent and losing_ground and lag_minutes > LAG_ALERT_MINUTES:
        new_value = notify_slack(
            f":hourglass: *Scarf export falling behind* ({DEPLOY_ENVIRONMENT})\n"
            f"The sync is {lag_minutes:.0f} minutes behind real time and not catching up: "
            f"windows are taking longer to sync than the {SYNC_INTERVAL_MINUTES} minutes "
            "they cover.\nNo further lag alerts will be sent until it catches up."
        )
    elif lag_alert_sent and caught_up:
        notify_slack(f":white_check_mark: *Scarf export caught up* ({DEPLOY_ENVIRONMENT}).")
        new_value = False
    else:
        return lag_alert_sent

    if new_value != lag_alert_sent:
        try:
            _patch_state({LAG_ALERT_SENT_KEY: "true" if new_value else "false"})
        except Exception as e:
            print(f"Warning: failed to persist lag alert state: {e}")
    return new_value


def determine_window(state):
    """Returns (start_ns, end_ns, caught_up); caught_up means the window ends at "now",
    so there is nothing left to sync after it."""
    now_ns = int(time.time() * 1e9)
    interval_ns = SYNC_INTERVAL_MINUTES * 60 * int(1e9)
    raw = (state or {}).get(CHECKPOINT_KEY)
    checkpoint_ns = int(raw) if raw else None

    if checkpoint_ns is None:
        print(f"No checkpoint found; using default {SYNC_INTERVAL_MINUTES}-minute window.")
        return now_ns - interval_ns, now_ns, True

    # Never query past "now"; a checkpoint within one interval of now yields a shorter window.
    end_ns = min(checkpoint_ns + interval_ns, now_ns)
    behind_minutes = (now_ns - end_ns) / 1e9 / 60
    if behind_minutes > 0:
        print(f"Catching up: this window ends {behind_minutes:.1f} minutes behind now.")
    return checkpoint_ns, end_ns, end_ns == now_ns


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
            raise SyncError(f"Error fetching logs from Loki: {e}") from e

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
    """Returns the errors of any batches that failed to ship; empty means all succeeded."""
    if not events:
        print("No telemetry events parsed in this window. Exiting cleanly.")
        return []

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
        ndjson_body = "\n".join(json.dumps(event) for event in batch).encode("utf-8")
        for attempt in range(1, SCARF_MAX_ATTEMPTS + 1):
            retry_after = None
            try:
                res = session.post(url, data=ndjson_body, headers=headers, timeout=SCARF_TIMEOUT)
                if res.status_code in RETRYABLE_STATUS_CODES:
                    retry_after = res.headers.get("Retry-After")
                res.raise_for_status()
                # HTTP 200 only means Scarf accepted the request, not that every event
                # ingested cleanly -- check the response body for per-event rejections
                try:
                    body = res.json()
                except ValueError:
                    body = res.text
                return batch_num, len(batch), body, None
            except Exception as e:
                response = getattr(e, "response", None)
                retryable = (
                    isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))
                    or (isinstance(e, requests.exceptions.HTTPError)
                        and response is not None
                        and (response.status_code in RETRYABLE_STATUS_CODES
                             # Scarf caps active imports at 15 per account; wait for some to drain.
                             or (response.status_code == 422
                                 and "too many active imports" in response.text.lower())))
                )
                if not retryable or attempt == SCARF_MAX_ATTEMPTS:
                    return batch_num, len(batch), None, e

                delay = SCARF_BACKOFF_BASE_SECONDS * 2 ** (attempt - 1)
                if retry_after and retry_after.isdigit():
                    delay = max(delay, int(retry_after))
                # Jitter keeps all workers from retrying in lockstep after a shared stall.
                delay += random.uniform(0, 1)
                print(f"Batch {batch_num}: attempt {attempt}/{SCARF_MAX_ATTEMPTS} failed ({e}); retrying in {delay:.1f}s.")
                time.sleep(delay)

    failed_batches = []
    with session, concurrent.futures.ThreadPoolExecutor(max_workers=SCARF_MAX_WORKERS) as executor:
        futures = [executor.submit(send_batch, i + 1, batch) for i, batch in enumerate(batches)]
        # Process completions as they arrive; continue draining the rest even
        # if some batches fail, to isolate processing failures per batch
        for future in concurrent.futures.as_completed(futures):
            batch_num, batch_len, body, error = future.result()
            if error is not None:
                print(f"Error shipping batch {batch_num} to Scarf: {error}")
                failed_batches.append(f"batch {batch_num}: {error}")
            else:
                print(f"Batch {batch_num}: Sent {batch_len} records. Response: {body}")

    return failed_batches


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
        raise SyncError(f"Missing required environment variable(s): {', '.join(missing)}")

def sync_window(start_ns, end_ns):
    """Fetches, parses and ships one window; raises SyncError if any of it failed."""
    stage_started_at = time.monotonic()
    loki_payload = fetch_loki_logs(start_ns, end_ns)
    print(f"Fetch stage took {time.monotonic() - stage_started_at:.1f}s.")

    stage_started_at = time.monotonic()
    telemetry_events = parse_telemetry(loki_payload)
    print(f"Parse stage took {time.monotonic() - stage_started_at:.1f}s.")

    stage_started_at = time.monotonic()
    failed_batches = ship_to_scarf(telemetry_events)
    print(f"Ship stage took {time.monotonic() - stage_started_at:.1f}s.")

    if failed_batches:
        raise SyncError(
            f"{len(failed_batches)} batch(es) failed to ship to Scarf; checkpoint left "
            f"unchanged so this window is retried. First error: {failed_batches[0]}"
        )


def main():
    run_started_at = time.monotonic()
    state = load_state()
    window = None
    windows_synced = 0
    try:
        validate_config()
        # Keep syncing window after window until caught up with "now". concurrencyPolicy:
        # Forbid skips the CronJob ticks that fire while a long catch-up is still running.
        while True:
            window_start_ns, window_end_ns, caught_up = determine_window(state)
            window = (window_start_ns, window_end_ns)
            print(f"Syncing window {_iso(window_start_ns)} to {_iso(window_end_ns)}.")
            lag_before_ns = time.time_ns() - window_start_ns
            sync_window(window_start_ns, window_end_ns)
            lag_after_ns = time.time_ns() - window_end_ns

            # Only advance the checkpoint once everything in the window shipped -- if a
            # batch failed, the next run re-covers the same window instead of silently
            # losing it (safe: Scarf dedupes on $unique_id).
            save_checkpoint(window_end_ns)
            windows_synced += 1
            if state and state.get(ALERT_SENT_KEY) == "true":
                notify_slack(
                    f":white_check_mark: *Scarf export recovered* ({DEPLOY_ENVIRONMENT}) after "
                    f"{state.get(FAILURE_COUNT_KEY, '?')} failed run(s)."
                )
            # Mirror what save_checkpoint persisted, so the next window starts from here and
            # a later failure in this run counts its streak from zero.
            state = {
                **(state or {}),
                CHECKPOINT_KEY: str(window_end_ns),
                FAILURE_COUNT_KEY: "0",
                ALERT_SENT_KEY: "false",
            }
            # Lag only shrinks if the window synced faster than the time it covers.
            lag_alert_sent = check_lag(state, lag_after_ns, lag_after_ns >= lag_before_ns, caught_up)
            state[LAG_ALERT_SENT_KEY] = "true" if lag_alert_sent else "false"
            if caught_up:
                break
    except Exception as e:
        if not isinstance(e, SyncError):
            traceback.print_exc()
        print(f"Error: {e}")
        record_failure(state, e, window)
        sys.exit(1)
    finally:
        # Runs even when the run fails, so every run -- successful or not -- reports
        # how long it took.
        print(f"Run finished in {time.monotonic() - run_started_at:.1f}s ({windows_synced} window(s) synced).")


if __name__ == "__main__":
    main()

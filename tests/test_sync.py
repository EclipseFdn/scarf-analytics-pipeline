"""Unit tests for scripts/sync.py.

Every external dependency is faked: the checkpoint ConfigMap (sync._k8s_request),
Loki (requests.get), Scarf (requests.Session.post), Slack (requests.post) and the
clock (sync.time), so the tests run offline and without real sleeps.
"""
import hashlib
import json
import os
import sys
import time as real_time
import types

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import sync  # noqa: E402

MINUTE_NS = 60 * 10**9
# A fixed "now", aligned to the minute so window boundaries are easy to reason about.
NOW_NS = 1_750_000_020 * 10**9


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None):
        self.status_code = status_code
        self._body = {} if body is None else body
        self.text = json.dumps(self._body)
        self.headers = headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} Error", response=self)


class FakeClock:
    """Stands in for the time module inside sync; sleep() advances the clock instantly."""

    def __init__(self, now_ns):
        self.now_ns = now_ns
        self.sleeps = []

    def time(self):
        return self.now_ns / 1e9

    def time_ns(self):
        return self.now_ns

    def monotonic(self):
        return self.now_ns / 1e9

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now_ns += int(seconds * 1e9)

    def advance(self, seconds):
        self.now_ns += int(seconds * 1e9)

    strftime = staticmethod(real_time.strftime)
    gmtime = staticmethod(real_time.gmtime)


class FakeConfigMap:
    """In-memory checkpoint ConfigMap behind sync._k8s_request."""

    def __init__(self, data=None):
        self.data = None if data is None else dict(data)
        self.fail_reads = False

    def request(self, method, path, **kwargs):
        if self.fail_reads and method == "GET":
            raise requests.exceptions.ConnectionError("cluster unreachable")
        if method == "GET":
            return FakeResponse(404) if self.data is None else FakeResponse(200, {"data": dict(self.data)})
        if method == "PATCH" and self.data is None:
            return FakeResponse(404)
        self.data = {**(self.data or {}), **kwargs["json"]["data"]}
        return FakeResponse(200)


def loki_line(ts_ns, **fields):
    fields.setdefault("client_ip", "203.0.113.7")
    fields.setdefault("request_user_agent", "curl/8.0")
    fields.setdefault("url", "/api/foo")
    return [str(ts_ns), json.dumps(fields)]


def loki_payload(values):
    return {"data": {"result": [{"stream": {"service_name": "fastly_cdn"}, "values": values}]}}


@pytest.fixture(autouse=True)
def no_slack_by_default(monkeypatch):
    """Keeps a SLACK_WEBHOOK_URL from the environment from being used; the slack
    fixture sets a fake one."""
    monkeypatch.setattr(sync, "SLACK_WEBHOOK_URL", None)


@pytest.fixture
def clock(monkeypatch):
    fake = FakeClock(NOW_NS)
    monkeypatch.setattr(sync, "time", fake)
    return fake


@pytest.fixture
def configmap(monkeypatch):
    fake = FakeConfigMap()
    monkeypatch.setattr(sync, "_k8s_request", fake.request)
    return fake


@pytest.fixture
def slack(monkeypatch):
    """Collects Slack messages; set .fail = True to make the webhook reject them."""
    sent = types.SimpleNamespace(messages=[], fail=False)

    def post(url, json=None, **kwargs):
        if sent.fail:
            return FakeResponse(500)
        sent.messages.append(json["text"])
        return FakeResponse(200)

    monkeypatch.setattr(sync, "SLACK_WEBHOOK_URL", "https://hooks.slack.test/abc")
    monkeypatch.setattr(sync.requests, "post", post)
    return sent


@pytest.fixture
def scarf(monkeypatch):
    """Fake Scarf import API. .responses is a queue of responses (or exceptions) to
    return; when empty, requests succeed. .calls records each posted batch."""
    fake = types.SimpleNamespace(responses=[], calls=[])

    def post(self, url, data=None, **kwargs):
        fake.calls.append(data.decode("utf-8").split("\n"))
        if fake.responses:
            resp = fake.responses.pop(0)
            if isinstance(resp, Exception):
                raise resp
            return resp
        return FakeResponse(200, {"accepted": True})

    monkeypatch.setattr(requests.Session, "post", post)
    return fake


@pytest.fixture
def config(monkeypatch):
    for name, value in {
        "LOKI_URL": "https://loki.test",
        "LOKI_QUERY": '{service_name="fastly_cdn"}',
        "SCARF_API_TOKEN": "token",
        "SCARF_ENTITY_ID": "entity",
        "DEPLOY_ENVIRONMENT": "test",
    }.items():
        monkeypatch.setattr(sync, name, value)


# --- parse_telemetry --- #

class TestParseTelemetry:
    def test_builds_scarf_event_from_log_line(self):
        value = loki_line(NOW_NS, url="/api/foo", request_method="GET", status=200)
        [event] = sync.parse_telemetry(loki_payload([value]))

        assert event["$type"] == "request"
        assert event["$unique_id"] == hashlib.sha256(value[1].encode("utf-8")).hexdigest()
        assert event["$remote_address"] == "203.0.113.7"
        assert event["$user_agent"] == "curl/8.0"
        assert event["$time"] == real_time.strftime("%Y-%m-%dT%H:%M:%SZ", real_time.gmtime(NOW_NS / 1e9))
        assert event["url"] == "/api/foo"
        assert event["request_method"] == "GET"
        assert event["status"] == 200

    def test_download_urls_are_typed_download_case_insensitively(self):
        [event] = sync.parse_telemetry(loki_payload([loki_line(NOW_NS, url="/vscode/Download/x.vsix")]))
        assert event["$type"] == "download"

    def test_missing_url_is_a_request_with_null_url(self):
        [event] = sync.parse_telemetry(loki_payload([loki_line(NOW_NS, url=None)]))
        assert event["$type"] == "request"
        assert event["url"] is None

    def test_drops_lines_without_ip_and_user_agent(self):
        values = [
            loki_line(NOW_NS, client_ip=None, request_user_agent=None),
            loki_line(NOW_NS, client_ip=None),
            loki_line(NOW_NS, request_user_agent=None),
        ]
        assert len(sync.parse_telemetry(loki_payload(values))) == 2

    def test_skips_unparseable_lines(self):
        values = [[str(NOW_NS), "not json"], loki_line(NOW_NS)]
        assert len(sync.parse_telemetry(loki_payload(values))) == 1

    def test_identical_lines_get_identical_unique_ids(self):
        value = loki_line(NOW_NS)
        first, second = sync.parse_telemetry(loki_payload([value, value]))
        assert first["$unique_id"] == second["$unique_id"]


# --- determine_window --- #

class TestDetermineWindow:
    @pytest.mark.parametrize("state", [None, {}])
    def test_no_checkpoint_uses_last_interval(self, clock, state):
        start, end, caught_up = sync.determine_window(state)
        assert (start, end, caught_up) == (NOW_NS - 5 * MINUTE_NS, NOW_NS, True)

    def test_checkpoint_far_behind_yields_one_full_interval(self, clock):
        checkpoint = NOW_NS - 60 * MINUTE_NS
        start, end, caught_up = sync.determine_window({sync.CHECKPOINT_KEY: str(checkpoint)})
        assert (start, end, caught_up) == (checkpoint, checkpoint + 5 * MINUTE_NS, False)

    def test_checkpoint_within_interval_is_capped_at_now(self, clock):
        checkpoint = NOW_NS - 2 * MINUTE_NS
        start, end, caught_up = sync.determine_window({sync.CHECKPOINT_KEY: str(checkpoint)})
        assert (start, end, caught_up) == (checkpoint, NOW_NS, True)


# --- fetch_loki_logs --- #

class TestFetchLokiLogs:
    def test_paginates_and_drops_entries_repeated_across_pages(self, monkeypatch, config):
        monkeypatch.setattr(sync, "LOKI_PAGE_LIMIT", 3)
        page1 = [loki_line(NOW_NS + i, n=i) for i in range(3)]
        # The next page starts at page 1's max timestamp, so its last entry comes back again.
        page2 = [page1[-1], loki_line(NOW_NS + 3, n=3)]
        pages = [loki_payload(page1), loki_payload(page2)]
        starts = []

        def get(url, params=None, **kwargs):
            starts.append(params["start"])
            return FakeResponse(200, pages.pop(0))

        monkeypatch.setattr(sync.requests, "get", get)
        result = sync.fetch_loki_logs(NOW_NS, NOW_NS + MINUTE_NS)

        values = [v for stream in result["data"]["result"] for v in stream["values"]]
        assert len(values) == 4
        assert starts == [NOW_NS, NOW_NS + 2]

    def test_stops_when_page_cannot_advance(self, monkeypatch, config):
        monkeypatch.setattr(sync, "LOKI_PAGE_LIMIT", 2)
        same_ns = [loki_line(NOW_NS, n=i) for i in range(2)]
        calls = []

        def get(url, params=None, **kwargs):
            calls.append(params)
            return FakeResponse(200, loki_payload(same_ns))

        monkeypatch.setattr(sync.requests, "get", get)
        sync.fetch_loki_logs(NOW_NS, NOW_NS + MINUTE_NS)
        assert len(calls) == 1

    def test_loki_error_raises_sync_error(self, monkeypatch, config):
        monkeypatch.setattr(sync.requests, "get", lambda *a, **k: FakeResponse(503))
        with pytest.raises(sync.SyncError, match="Error fetching logs from Loki"):
            sync.fetch_loki_logs(NOW_NS, NOW_NS + MINUTE_NS)


# --- ship_to_scarf --- #

class TestShipToScarf:
    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch, config, clock):
        monkeypatch.setattr(sync, "SCARF_BATCH_SIZE", 2)
        monkeypatch.setattr(sync, "SCARF_MAX_WORKERS", 1)

    def events(self, n):
        return [{"$unique_id": str(i)} for i in range(n)]

    def test_no_events_is_success_without_calls(self, scarf):
        assert sync.ship_to_scarf([]) == []
        assert scarf.calls == []

    def test_splits_events_into_ndjson_batches(self, scarf):
        assert sync.ship_to_scarf(self.events(5)) == []
        assert sorted(len(batch) for batch in scarf.calls) == [1, 2, 2]
        assert all(json.loads(line) for batch in scarf.calls for line in batch)

    @pytest.mark.parametrize("transient", [
        FakeResponse(503),
        FakeResponse(429, headers={"Retry-After": "7"}),
        FakeResponse(422, {"error": "Too many active imports"}),
        requests.exceptions.Timeout("read timed out"),
        requests.exceptions.ConnectionError("connection reset"),
    ])
    def test_retries_transient_failures(self, scarf, clock, transient):
        scarf.responses = [transient]
        assert sync.ship_to_scarf(self.events(1)) == []
        assert len(scarf.calls) == 2
        assert len(clock.sleeps) == 1

    def test_honours_retry_after_when_longer_than_backoff(self, scarf, clock):
        scarf.responses = [FakeResponse(429, headers={"Retry-After": "30"})]
        sync.ship_to_scarf(self.events(1))
        assert 30 <= clock.sleeps[0] < 31

    def test_does_not_retry_non_retryable_errors(self, scarf, clock):
        scarf.responses = [FakeResponse(401, {"error": "bad token"})]
        [error] = sync.ship_to_scarf(self.events(1))
        assert "batch 1" in error and "401" in error
        assert len(scarf.calls) == 1
        assert clock.sleeps == []

    def test_gives_up_after_max_attempts(self, scarf, clock):
        scarf.responses = [FakeResponse(503)] * sync.SCARF_MAX_ATTEMPTS
        assert len(sync.ship_to_scarf(self.events(1))) == 1
        assert len(scarf.calls) == sync.SCARF_MAX_ATTEMPTS

    def test_other_batches_still_ship_when_one_fails(self, scarf):
        scarf.responses = [FakeResponse(400)]
        errors = sync.ship_to_scarf(self.events(4))
        assert len(errors) == 1
        assert len(scarf.calls) == 2


# --- validate_config --- #

def test_validate_config_reports_all_missing_variables(monkeypatch, config):
    monkeypatch.setattr(sync, "LOKI_URL", None)
    monkeypatch.setattr(sync, "SCARF_ENTITY_ID", "")
    with pytest.raises(sync.SyncError, match="LOKI_URL, SCARF_ENTITY_ID"):
        sync.validate_config()


# --- state persistence --- #

class TestState:
    def test_load_state_first_run_is_empty(self, configmap):
        assert sync.load_state() == {}

    def test_load_state_unreadable_is_none(self, configmap):
        configmap.fail_reads = True
        assert sync.load_state() is None

    def test_save_checkpoint_creates_configmap_and_resets_failures(self, configmap):
        sync.save_checkpoint(NOW_NS)
        assert configmap.data[sync.CHECKPOINT_KEY] == str(NOW_NS)
        assert configmap.data[sync.FAILURE_COUNT_KEY] == "0"
        assert configmap.data[sync.ALERT_SENT_KEY] == "false"

    def test_save_checkpoint_keeps_other_keys(self, configmap):
        configmap.data = {sync.LAG_ALERT_SENT_KEY: "true"}
        sync.save_checkpoint(NOW_NS)
        assert configmap.data[sync.LAG_ALERT_SENT_KEY] == "true"


# --- notify_slack --- #

class TestNotifySlack:
    def test_disabled_without_webhook(self, monkeypatch):
        monkeypatch.setattr(sync, "SLACK_WEBHOOK_URL", None)
        monkeypatch.setattr(sync.requests, "post", lambda *a, **k: pytest.fail("should not post"))
        assert sync.notify_slack("hi") is False

    def test_delivered(self, slack):
        assert sync.notify_slack("hi") is True
        assert slack.messages == ["hi"]

    def test_webhook_error_is_swallowed(self, slack):
        slack.fail = True
        assert sync.notify_slack("hi") is False


# --- record_failure --- #

class TestRecordFailure:
    WINDOW = (NOW_NS - 5 * MINUTE_NS, NOW_NS)

    def test_first_failure_alerts_and_is_recorded(self, configmap, slack, config):
        sync.record_failure({}, sync.SyncError("boom"), self.WINDOW)
        [message] = slack.messages
        assert "Scarf export failing" in message and "boom" in message
        assert configmap.data[sync.FAILURE_COUNT_KEY] == "1"
        assert configmap.data[sync.ALERT_SENT_KEY] == "true"

    def test_does_not_alert_again_during_same_outage(self, configmap, slack, config):
        state = {sync.FAILURE_COUNT_KEY: "3", sync.ALERT_SENT_KEY: "true"}
        sync.record_failure(state, sync.SyncError("boom"), self.WINDOW)
        assert slack.messages == []
        assert configmap.data[sync.FAILURE_COUNT_KEY] == "4"
        assert configmap.data[sync.ALERT_SENT_KEY] == "true"

    def test_waits_for_threshold(self, monkeypatch, configmap, slack, config):
        monkeypatch.setattr(sync, "ALERT_AFTER_CONSECUTIVE_FAILURES", 3)
        state = {}
        for _ in range(3):
            sync.record_failure(state, sync.SyncError("boom"), self.WINDOW)
            state = dict(configmap.data)
        assert len(slack.messages) == 1
        assert "*Consecutive failed runs:* 3" in slack.messages[0]

    def test_undelivered_alert_is_retried_next_run(self, configmap, slack, config):
        slack.fail = True
        sync.record_failure({}, sync.SyncError("boom"), self.WINDOW)
        assert configmap.data[sync.ALERT_SENT_KEY] == "false"

    def test_unreadable_state_alerts_anyway(self, configmap, slack, config):
        sync.record_failure(None, sync.SyncError("boom"), None)
        [message] = slack.messages
        assert "*Consecutive failed runs:* unknown" in message
        assert "*Window:* not determined" in message
        assert configmap.data is None  # don't overwrite state we couldn't read


# --- check_lag --- #

class TestCheckLag:
    def test_alerts_when_behind_and_losing_ground(self, configmap, slack, config):
        assert sync.check_lag({}, 90 * MINUTE_NS, losing_ground=True, caught_up=False) is True
        [message] = slack.messages
        assert "falling behind" in message and "90 minutes" in message
        assert configmap.data[sync.LAG_ALERT_SENT_KEY] == "true"

    def test_no_alert_when_behind_but_gaining(self, configmap, slack, config):
        assert sync.check_lag({}, 90 * MINUTE_NS, losing_ground=False, caught_up=False) is False
        assert slack.messages == []

    def test_no_alert_under_threshold(self, configmap, slack, config):
        assert sync.check_lag({}, 30 * MINUTE_NS, losing_ground=True, caught_up=False) is False
        assert slack.messages == []

    def test_no_repeat_alert(self, configmap, slack, config):
        state = {sync.LAG_ALERT_SENT_KEY: "true"}
        assert sync.check_lag(state, 120 * MINUTE_NS, losing_ground=True, caught_up=False) is True
        assert slack.messages == []

    def test_caught_up_message_clears_flag(self, configmap, slack, config):
        state = {sync.LAG_ALERT_SENT_KEY: "true"}
        assert sync.check_lag(state, 0, losing_ground=False, caught_up=True) is False
        assert "caught up" in slack.messages[0]
        assert configmap.data[sync.LAG_ALERT_SENT_KEY] == "false"


# --- main (end-to-end over the fakes) --- #

class TestMain:
    @pytest.fixture
    def loki(self, monkeypatch, clock):
        """Fake Loki returning one log line per window; .cost_seconds(n) sets how long
        the n-th window takes to fetch, advancing the fake clock."""
        fake = types.SimpleNamespace(windows=[], cost_seconds=lambda n: 1)

        def get(url, params=None, **kwargs):
            fake.windows.append((params["start"], params["end"]))
            clock.advance(fake.cost_seconds(len(fake.windows)))
            return FakeResponse(200, loki_payload([loki_line(params["start"])]))

        monkeypatch.setattr(sync.requests, "get", get)
        return fake

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch, config, clock, configmap, slack, scarf):
        monkeypatch.setattr(sync, "SCARF_MAX_ATTEMPTS", 1)

    def test_catches_up_five_minutes_at_a_time(self, loki, configmap, clock):
        configmap.data = {sync.CHECKPOINT_KEY: str(NOW_NS - 60 * MINUTE_NS)}
        sync.main()

        assert len(loki.windows) == 13  # 12 full windows + the tail up to "now"
        for start, end in loki.windows[:12]:
            assert end - start == 5 * MINUTE_NS
        for (_, prev_end), (next_start, _) in zip(loki.windows, loki.windows[1:]):
            assert next_start == prev_end  # contiguous: nothing skipped or re-queried
        assert configmap.data[sync.CHECKPOINT_KEY] == str(loki.windows[-1][1])

    def test_first_run_syncs_last_interval(self, loki, configmap):
        sync.main()
        assert loki.windows == [(NOW_NS - 5 * MINUTE_NS, NOW_NS)]
        assert configmap.data[sync.CHECKPOINT_KEY] == str(NOW_NS)

    def test_failure_keeps_progress_and_exits_nonzero(self, loki, configmap, scarf, slack):
        configmap.data = {sync.CHECKPOINT_KEY: str(NOW_NS - 60 * MINUTE_NS)}
        # Windows 1-3 ship, window 4 is rejected.
        scarf.responses = [FakeResponse(200)] * 3 + [FakeResponse(401)]

        with pytest.raises(SystemExit) as exit_info:
            sync.main()

        assert exit_info.value.code == 1
        assert len(loki.windows) == 4
        assert configmap.data[sync.CHECKPOINT_KEY] == str(loki.windows[2][1])
        assert configmap.data[sync.FAILURE_COUNT_KEY] == "1"
        assert len(slack.messages) == 1 and "Scarf export failing" in slack.messages[0]

    def test_recovery_message_after_alerted_outage(self, loki, configmap, slack):
        configmap.data = {
            sync.CHECKPOINT_KEY: str(NOW_NS - 10 * MINUTE_NS),
            sync.FAILURE_COUNT_KEY: "4",
            sync.ALERT_SENT_KEY: "true",
        }
        sync.main()
        assert slack.messages == [":white_check_mark: *Scarf export recovered* (test) after 4 failed run(s)."]
        assert configmap.data[sync.ALERT_SENT_KEY] == "false"

    def test_missing_config_fails_and_alerts(self, monkeypatch, loki, slack):
        monkeypatch.setattr(sync, "SCARF_API_TOKEN", None)
        with pytest.raises(SystemExit):
            sync.main()
        assert loki.windows == []
        assert "SCARF_API_TOKEN" in slack.messages[0]

    def test_unexpected_exception_fails_and_alerts(self, monkeypatch, loki, slack):
        monkeypatch.setattr(sync, "parse_telemetry", lambda payload: 1 / 0)
        with pytest.raises(SystemExit):
            sync.main()
        assert "division by zero" in slack.messages[0]

    def test_slow_windows_trigger_lag_alert_then_caught_up(self, loki, configmap, slack):
        configmap.data = {sync.CHECKPOINT_KEY: str(NOW_NS - 120 * MINUTE_NS)}
        # First 3 windows each take 6 minutes (longer than the 5 they cover), then fast.
        loki.cost_seconds = lambda n: 360 if n <= 3 else 1
        sync.main()
        assert len(slack.messages) == 2
        assert "falling behind" in slack.messages[0]
        assert "caught up" in slack.messages[1]

    def test_fast_backlog_does_not_trigger_lag_alert(self, loki, configmap, slack):
        configmap.data = {sync.CHECKPOINT_KEY: str(NOW_NS - 120 * MINUTE_NS)}
        sync.main()
        assert slack.messages == []

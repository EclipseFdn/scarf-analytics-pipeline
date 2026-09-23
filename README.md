# scarf-analytics-pipeline

A scheduled pipeline that pulls Fastly CDN traffic logs from Grafana Loki and
ships them as telemetry events to [Scarf](https://scarf.sh), giving OpenVSX
visibility into package download and general request analytics.

## How it works

A Kubernetes `CronJob` runs [`scripts/sync.py`](scripts/sync.py) on a fixed
interval (default: every 5 minutes). Each run:

1. **Determines the query window.** The end of the last successful run is
   read from a checkpoint `ConfigMap` (`scarf-sync-checkpoint` by default).
   If no checkpoint exists yet, it falls back to a fixed-size window
   (`SYNC_INTERVAL_MINUTES`). If the checkpoint is older than
   `MAX_LOOKBACK_MINUTES` (e.g. after an outage), the window is clamped so a
   long gap doesn't turn into one huge Loki query — the untracked span is
   permanently skipped.
2. **Fetches logs from Loki** via `query_range` for that window.
3. **Parses each log line** into a Scarf event, typed `download` or `request`
   depending on whether the URL looks like a download, deduplicated by a
   SHA-256 hash of the raw log line (`$unique_id`), and filters out lines
   missing both an IP and a user agent.
4. **Ships events to Scarf** in batches (`SCARF_BATCH_SIZE`, default 500) via
   the Scarf v2 import API.
5. **Advances the checkpoint** only if every batch shipped successfully. If
   any batch fails, the checkpoint is left unchanged so the next run retries
   the same window (safe, since Scarf dedupes on `$unique_id`).

## Repository layout

```
scripts/
  sync.py             # The sync job itself
  requirements.txt    # Python dependencies
charts/               # Helm chart deploying the CronJob, RBAC, etc.
kubernetes/
  helm-deploy.sh       # Deploys the Helm chart to a target environment
  namespace-rbac.yaml  # Namespace-scoped RBAC needed for Jenkins to deploy
Dockerfile            # Multi-stage build producing the sync-job image
Jenkinsfile           # CI: build, push, and deploy to staging on main
```

## Configuration

`sync.py` is configured entirely through environment variables, set via
[`charts/values.yaml`](charts/values.yaml) and a `scarf-loki-credentials`
secret in the target namespace.

| Variable | Description | Default |
|---|---|---|
| `LOKI_URL` | Base URL of the Grafana Loki instance (secret) | — |
| `LOKI_USER` | Loki basic-auth username (secret) | — |
| `LOKI_API_KEY` | Loki basic-auth API key (secret) | — |
| `LOKI_QUERY` | LogQL query selecting the log stream to sync | — |
| `SCARF_API_TOKEN` | Scarf API token (secret) | — |
| `SCARF_ENTITY_ID` | Scarf package/entity ID to import events into | — |
| `ORGANIZATION_NAME` | Scarf organization name | `OpenVSX` |
| `SCARF_BATCH_SIZE` | Max events per Scarf import request | `500` |
| `SYNC_INTERVAL_MINUTES` | Window size for the first run (no checkpoint yet); must match the CronJob schedule | `5` |
| `MAX_LOOKBACK_MINUTES` | Caps how far a resumed run will query back if the checkpoint is stale | `60` |
| `CHECKPOINT_CONFIGMAP_NAME` | Name of the ConfigMap used to persist the sync checkpoint | `scarf-sync-checkpoint` |

`LOKI_URL`, `LOKI_USER`, `LOKI_API_KEY`, `SCARF_API_TOKEN`, and
`SCARF_ENTITY_ID` are expected to come from the `scarf-loki-credentials`
Kubernetes secret (referenced via `envFrom` in the CronJob template) rather
than `values.yaml`.

## Running locally

```bash
pip install -r scripts/requirements.txt

export LOKI_URL=https://logs-prod-018.grafana.net/
export LOKI_USER=...
export LOKI_API_KEY=...
export LOKI_QUERY='{service_name="fastly_cdn", env="production"}'
export SCARF_API_TOKEN=...
export SCARF_ENTITY_ID=...

python scripts/sync.py
```

Outside a cluster, checkpoint reads/writes fail gracefully (no in-cluster
service account is available), so each local run falls back to the default
`SYNC_INTERVAL_MINUTES` window.

## Deployment

The Helm chart in [`charts/`](charts) deploys the CronJob along with a
dedicated `ServiceAccount` and a `Role`/`RoleBinding` scoped to just the
checkpoint `ConfigMap`.

```bash
./kubernetes/helm-deploy.sh staging <docker_image_tag>
```

See [`kubernetes/README.md`](kubernetes/README.md) for one-time cluster
setup (applying `namespace-rbac.yaml` for Jenkins).

CI/CD is handled by the [`Jenkinsfile`](Jenkinsfile): every build produces
and pushes a Docker image to `ghcr.io/eclipsefdn/scarf-analytics`, and
pushes to `main` automatically deploy to staging via `helm-deploy.sh`.

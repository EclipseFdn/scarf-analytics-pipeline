#!/usr/bin/env bash

#*******************************************************************************
# Copyright (c) 2024 Eclipse Foundation and others.
# This program and the accompanying materials are made available
# under the terms of the Eclipse Public License 2.0
# which is available at http://www.eclipse.org/legal/epl-v20.html
# SPDX-License-Identifier: EPL-2.0
#*******************************************************************************

# Bash strict-mode
set -o errexit
set -o nounset
set -o pipefail

IFS=$'\n\t'
SCRIPT_FOLDER="$(dirname "$(readlink -f "${0}")")"
ROOT_DIR="${SCRIPT_FOLDER}/.."
CHART_DIR="${ROOT_DIR}/charts"

release_name="staging"
namespace="analytics-pipeline"

environment="${1:-}"
image_tag="${2:-}"

# DRY_RUN=1 ./helm-deploy.sh ... → render + server-side validate, do not apply.
DRY_RUN="${DRY_RUN:-}"

if [[ -z "${environment}" ]]; then
  printf "ERROR: an environment must be given.\n"
  exit 1
fi

if [[ "${environment}" != "staging" ]]; then
  printf "ERROR: Unknown environment '%s'. Only 'staging' is currently supported.\n" "${environment}"
  exit 1
fi

if [[ -z "${image_tag}" ]]; then
  printf "ERROR: an image_tag must be given.\n"
  exit 1
fi

chmod 600 "${KUBECONFIG}"

export HELM_CACHE_HOME="${ROOT_DIR}/.helm/cache"
export HELM_CONFIG_HOME="${ROOT_DIR}/.helm/config"
export HELM_DATA_HOME="${ROOT_DIR}/.helm/data"

mkdir -p "${HELM_CACHE_HOME}"
mkdir -p "${HELM_CONFIG_HOME}"
mkdir -p "${HELM_DATA_HOME}"

if [[ -n "${DRY_RUN}" ]]; then
  printf "==> DRY RUN — render + server-side validate, no changes will be applied\n"
  helm_mode_flags=(--dry-run=server --debug)
else
  helm_mode_flags=(--atomic --timeout 10m)
fi

helm version

printf "==> Running helm upgrade: release='%s' namespace='%s' image_tag='%s'\n" "${release_name}" "${namespace}" "${image_tag}"
helm upgrade --install "${release_name}" "${CHART_DIR}" \
  --set image.tag="${image_tag}" \
  --namespace "${namespace}" \
  --create-namespace \
  "${helm_mode_flags[@]}"

if [[ -n "${DRY_RUN}" ]]; then
  printf "==> DRY RUN complete — no rollout to verify, exiting cleanly\n"
  exit 0
fi

printf "==> Verifying CronJob was created: cronjob/scarf-loki-sync in namespace '%s'\n" "${namespace}"
kubectl get cronjob scarf-loki-sync --namespace "${namespace}"

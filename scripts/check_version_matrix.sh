#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TASK="${TASK:-compileJava}"
MATRIX_FILE="${ROOT_DIR}/compatibility/matrix.tsv"

run_profile() {
  local id="$1"
  local profile="${ROOT_DIR}/compatibility/${id}.properties"
  if [[ ! -f "${profile}" ]]; then
    echo "[matrix] missing profile: ${profile}" >&2
    return 1
  fi

  local loader="forge"
  local props=()
  while IFS='=' read -r key value; do
    [[ -z "${key}" || "${key}" == \#* ]] && continue
    if [[ "${key}" == "loader" ]]; then
      loader="${value}"
      continue
    fi
    props+=("-P${key}=${value}")
  done < "${profile}"

  if [[ "${loader}" == "neoforge" ]]; then
    echo "[matrix] ${id}: NeoForge profile recorded, but this checkout uses ForgeGradle/net.minecraftforge sources."
    echo "[matrix] ${id}: create a compat/neoforge-${id#neoforge-} source branch before claiming a build."
    return 2
  fi

  echo "[matrix] ${id}: ./gradlew ${TASK} ${props[*]}"
  (cd "${ROOT_DIR}" && ./gradlew "${TASK}" "${props[@]}")
}

echo "[matrix] using ${MATRIX_FILE}"
profiles=()

if [[ -n "${PROFILE:-}" ]]; then
  IFS=',' read -r -a profiles <<< "${PROFILE}"
else
  profiles+=("local-1.21.1")
  if [[ "${CHECK_LATEST:-0}" == "1" ]]; then
    profiles+=("forge-1.21.1")
  fi
  if [[ "${CHECK_ALL:-0}" == "1" ]]; then
    profiles=()
    while IFS= read -r profile; do
      profiles+=("$(basename "${profile}" .properties)")
    done < <(find "${ROOT_DIR}/compatibility" -maxdepth 1 -name '*.properties' | sort)
  fi
fi

status=0
for id in "${profiles[@]}"; do
  if run_profile "${id}"; then
    :
  else
    profile_status=$?
    status=${profile_status}
    echo "[matrix] ${id}: failed with status ${profile_status}" >&2
  fi
done

exit "${status}"

#!/usr/bin/env bash
#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# Capture the candidate SQL runtime grammar and validate it with the paired
# OpenSearch Dashboards (OSD) headless PPL linter.

set -euo pipefail

SQL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SQL_ROOT"

HEADLESS_MODULE="src/plugins/data/public/antlr/opensearch_ppl/headless_ppl_lint"
VALIDATOR="$SQL_ROOT/scripts/ppl-lint/validate-osd-grammar.mjs"

TARGET_INPUT="${PPL_LINT_TARGET:-}"
OSD_ROOT_INPUT="${OSD_SOURCE_PATH:-}"
TARGET_BRANCH_INPUT="${TARGET_BRANCH:-${SQL_TARGET_BRANCH:-}}"
OSD_REPOSITORY_INPUT="${OSD_REPOSITORY:-}"
OSD_REF_INPUT="${OSD_REF:-}"

GRAMMAR_BUNDLE="${PPL_LINT_GRAMMAR_BUNDLE:-$SQL_ROOT/ppl-grammar-bundle.json}"
GRAMMAR_CASES="${PPL_LINT_CASES:-$SQL_ROOT/scripts/ppl-lint/grammar-cases.json}"
REPORT="${PPL_LINT_REPORT:-$SQL_ROOT/ppl-lint-grammar-compatibility-report.json}"
SUMMARY="${PPL_LINT_SUMMARY:-${GITHUB_STEP_SUMMARY:-$SQL_ROOT/ppl-lint-grammar-summary.md}}"
CLUSTER_LOG="${PPL_LINT_CLUSTER_LOG:-$SQL_ROOT/ppl-grammar-cluster.log}"
STARTUP_TIMEOUT="${PPL_LINT_STARTUP_TIMEOUT_SECONDS:-300}"
SKIP_OSD_BOOTSTRAP="${PPL_LINT_SKIP_OSD_BOOTSTRAP:-0}"
RELEASE_LINE_BYPASS=false

GRADLE_PID=""
CAPTURE_TMP=""

log() {
  printf '[ppl-lint-rule-validation] %s\n' "$*"
}

die() {
  log "ERROR: $*" >&2
  exit 2
}

usage() {
  cat <<'EOF'
Usage:
  scripts/ppl-lint-rule-validation.sh --target FILE --osd-root DIR
  scripts/ppl-lint-rule-validation.sh --osd-root DIR [--target-branch BRANCH]
      [--osd-repository OWNER/REPO] [--osd-ref REF]

Options:
  --target FILE          Existing resolved-target.json from CI.
  --osd-root DIR         Existing OSD checkout (or OSD_SOURCE_PATH).
  --target-branch NAME   SQL target branch for local metadata (main or X.Y).
  --osd-repository NAME  OSD repository recorded in local metadata.
  --osd-ref REF          OSD ref recorded in local metadata.
  --grammar FILE         Captured grammar output path.
  --cases FILE           Grammar cases input path.
  --report FILE          Validation report output path.
  --summary FILE         Validation summary output path.
  --cluster-log FILE     Gradle run log output path.
  --startup-timeout SEC  Bounded cluster readiness timeout (default: 300).
  -h, --help             Show this help.

Environment:
  PPL_LINT_SKIP_OSD_BOOTSTRAP=1
                         Skip OSD bootstrap on the supported path. The caller
                         must ensure generated OSD targets match the checkout.
EOF
}

require_value() {
  local option="$1"
  local value="${2:-}"
  [[ -n "$value" ]] || die "$option requires a value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      require_value "$1" "${2:-}"
      TARGET_INPUT="$2"
      shift 2
      ;;
    --osd-root)
      require_value "$1" "${2:-}"
      OSD_ROOT_INPUT="$2"
      shift 2
      ;;
    --target-branch)
      require_value "$1" "${2:-}"
      TARGET_BRANCH_INPUT="$2"
      shift 2
      ;;
    --osd-repository)
      require_value "$1" "${2:-}"
      OSD_REPOSITORY_INPUT="$2"
      shift 2
      ;;
    --osd-ref)
      require_value "$1" "${2:-}"
      OSD_REF_INPUT="$2"
      shift 2
      ;;
    --grammar)
      require_value "$1" "${2:-}"
      GRAMMAR_BUNDLE="$2"
      shift 2
      ;;
    --cases)
      require_value "$1" "${2:-}"
      GRAMMAR_CASES="$2"
      shift 2
      ;;
    --report)
      require_value "$1" "${2:-}"
      REPORT="$2"
      shift 2
      ;;
    --summary)
      require_value "$1" "${2:-}"
      SUMMARY="$2"
      shift 2
      ;;
    --cluster-log)
      require_value "$1" "${2:-}"
      CLUSTER_LOG="$2"
      shift 2
      ;;
    --startup-timeout)
      require_value "$1" "${2:-}"
      STARTUP_TIMEOUT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

absolute_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$SQL_ROOT" "$1" ;;
  esac
}

normalize_version() {
  local raw="$1"
  local normalized="${raw%%[-+]*}"
  if [[ ! "$normalized" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    die "invalid product version: $raw"
  fi
  if [[ "$raw" != "$normalized" && "$raw" != "$normalized"-* && "$raw" != "$normalized"+* ]]; then
    die "invalid product version: $raw"
  fi
  printf '%s\n' "$normalized"
}

release_line() {
  local version="$1"
  printf '%s\n' "${version%.*}"
}

read_sql_version() {
  local version
  version="$(
    sed -nE \
      's/.*opensearch_version = System\.getProperty\("opensearch\.version", "([^"]+)"\).*/\1/p' \
      "$SQL_ROOT/build.gradle"
  )"
  [[ -n "$version" && "$version" != *$'\n'* ]] ||
    die "could not read one default opensearch.version from build.gradle"
  printf '%s\n' "$version"
}

stop_cluster() {
  local pid="$GRADLE_PID"
  local count=0

  [[ -n "$pid" ]] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    log "Stopping Gradle development cluster (pid $pid)"
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null && [[ "$count" -lt 30 ]]; do
      sleep 1
      count=$((count + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      log "Gradle process did not stop in time; terminating it"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
  wait "$pid" 2>/dev/null || true
  GRADLE_PID=""
}

cleanup() {
  local status=$?
  trap - EXIT
  stop_cluster
  if [[ -n "$CAPTURE_TMP" ]]; then
    rm -f "$CAPTURE_TMP"
  fi
  exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in curl git jq node; do
  command -v "$command" >/dev/null 2>&1 || die "required command not found: $command"
done
[[ "$STARTUP_TIMEOUT" =~ ^[1-9][0-9]*$ ]] ||
  die "--startup-timeout must be a positive integer"
[[ "$SKIP_OSD_BOOTSTRAP" == "0" || "$SKIP_OSD_BOOTSTRAP" == "1" ]] ||
  die "PPL_LINT_SKIP_OSD_BOOTSTRAP must be 0 or 1"

GRAMMAR_BUNDLE="$(absolute_path "$GRAMMAR_BUNDLE")"
GRAMMAR_CASES="$(absolute_path "$GRAMMAR_CASES")"
REPORT="$(absolute_path "$REPORT")"
SUMMARY="$(absolute_path "$SUMMARY")"
CLUSTER_LOG="$(absolute_path "$CLUSTER_LOG")"

if [[ -n "$TARGET_INPUT" ]]; then
  TARGET="$(absolute_path "$TARGET_INPUT")"
  [[ -f "$TARGET" ]] || die "target metadata not found: $TARGET"
  jq -e '
    type == "object" and
    (.sql | type == "object") and
    (.sql.sha | type == "string" and length > 0) and
    (.sql.targetBranch | type == "string" and length > 0) and
    (.sql.version | type == "string" and length > 0) and
    (.osd | type == "object") and
    (.osd.repository | type == "string" and length > 0) and
    (.osd.ref | type == "string" and length > 0) and
    (.osd.sha | type == "string" and length > 0) and
    (.osd.version | type == "string" and length > 0) and
    ((has("releaseLineValidationBypassed") | not) or
      (.releaseLineValidationBypassed | type == "boolean"))
  ' "$TARGET" >/dev/null || die "target metadata is missing required SQL/OSD fields: $TARGET"
  OSD_REPOSITORY_INPUT="$(jq -r '.osd.repository' "$TARGET")"
  OSD_REF_INPUT="$(jq -r '.osd.ref' "$TARGET")"
  OSD_CHECKOUT_REF="$(jq -r '.osd.sha' "$TARGET")"
  RELEASE_LINE_BYPASS="$(jq -r '.releaseLineValidationBypassed // false' "$TARGET")"
else
  TARGET_BRANCH_INPUT="${TARGET_BRANCH_INPUT:-main}"
  OSD_REPOSITORY_INPUT="${OSD_REPOSITORY_INPUT:-opensearch-project/OpenSearch-Dashboards}"
  OSD_REF_INPUT="${OSD_REF_INPUT:-$TARGET_BRANCH_INPUT}"
  OSD_CHECKOUT_REF="$OSD_REF_INPUT"
  TARGET="$SQL_ROOT/resolved-target.json"
fi

if [[ -n "$OSD_ROOT_INPUT" ]]; then
  OSD_ROOT="$(cd "$OSD_ROOT_INPUT" 2>/dev/null && pwd)" ||
    die "OSD checkout not found: $OSD_ROOT_INPUT"
else
  OSD_ROOT="$SQL_ROOT/.ci/OpenSearch-Dashboards"
  OSD_REPO_URL="${OSD_REPO_URL:-https://github.com/$OSD_REPOSITORY_INPUT.git}"
  if [[ ! -d "$OSD_ROOT" ]]; then
    log "Creating managed OSD checkout for $OSD_REPOSITORY_INPUT"
    mkdir -p "$(dirname "$OSD_ROOT")"
    git clone --filter=blob:none --no-checkout --depth 1 "$OSD_REPO_URL" "$OSD_ROOT"
  else
    git -C "$OSD_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
      die "managed OSD path is not a Git checkout: $OSD_ROOT"
    if git -C "$OSD_ROOT" remote get-url origin >/dev/null 2>&1; then
      git -C "$OSD_ROOT" remote set-url origin "$OSD_REPO_URL"
    else
      git -C "$OSD_ROOT" remote add origin "$OSD_REPO_URL"
    fi
  fi
  log "Resolving managed OSD checkout at $OSD_REPOSITORY_INPUT@$OSD_CHECKOUT_REF"
  git -C "$OSD_ROOT" fetch --depth 1 origin "$OSD_CHECKOUT_REF"
  git -C "$OSD_ROOT" checkout --detach FETCH_HEAD
  OSD_ROOT="$(cd "$OSD_ROOT" && pwd)"
fi

git -C "$OSD_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
  die "OSD root is not a Git checkout: $OSD_ROOT"
[[ -f "$OSD_ROOT/package.json" ]] || die "OSD package.json not found under $OSD_ROOT"

SQL_SHA_ACTUAL="$(git -C "$SQL_ROOT" rev-parse HEAD)"
SQL_VERSION_RAW_ACTUAL="$(read_sql_version)"
SQL_VERSION_ACTUAL="$(normalize_version "$SQL_VERSION_RAW_ACTUAL")"
OSD_SHA_ACTUAL="$(git -C "$OSD_ROOT" rev-parse HEAD)"
OSD_VERSION_RAW_ACTUAL="$(jq -er '.version | select(type == "string" and length > 0)' "$OSD_ROOT/package.json")" ||
  die "could not read OSD package.json version"
OSD_VERSION_ACTUAL="$(normalize_version "$OSD_VERSION_RAW_ACTUAL")"

if [[ -z "$TARGET_INPUT" && -n "$OSD_ROOT_INPUT" ]]; then
  OSD_REF_SHA="$(git -C "$OSD_ROOT" rev-parse --verify "${OSD_REF_INPUT}^{commit}" 2>/dev/null)" ||
    die "OSD ref $OSD_REF_INPUT is not available in $OSD_ROOT"
  [[ "$OSD_REF_SHA" == "$OSD_SHA_ACTUAL" ]] ||
    die "OSD ref $OSD_REF_INPUT resolves to $OSD_REF_SHA, not checkout $OSD_SHA_ACTUAL"
fi

if [[ -z "$TARGET_INPUT" ]]; then
  [[ "$TARGET_BRANCH_INPUT" == "main" || "$TARGET_BRANCH_INPUT" =~ ^[0-9]+\.[0-9]+$ ]] ||
    die "local --target-branch must be main or an exact X.Y release branch"
  OSD_OVERRIDE=false
  if [[ "$OSD_REPOSITORY_INPUT" != "opensearch-project/OpenSearch-Dashboards" ||
    "$OSD_REF_INPUT" != "$TARGET_BRANCH_INPUT" ]]; then
    OSD_OVERRIDE=true
  fi
  target_tmp="$(mktemp "$TARGET.tmp.XXXXXX")"
  jq -n \
    --arg sqlSha "$SQL_SHA_ACTUAL" \
    --arg sqlHeadSha "${SQL_HEAD_SHA:-}" \
    --arg targetBranch "$TARGET_BRANCH_INPUT" \
    --arg sqlVersionRaw "$SQL_VERSION_RAW_ACTUAL" \
    --arg sqlVersion "$SQL_VERSION_ACTUAL" \
    --arg osdRepository "$OSD_REPOSITORY_INPUT" \
    --arg osdRef "$OSD_REF_INPUT" \
    --arg osdSha "$OSD_SHA_ACTUAL" \
    --arg osdVersion "$OSD_VERSION_ACTUAL" \
    --argjson osdOverride "$OSD_OVERRIDE" \
    '{
      schemaVersion: 1,
      sql: {
        sha: $sqlSha,
        targetBranch: $targetBranch,
        versionRaw: $sqlVersionRaw,
        version: $sqlVersion
      },
      osd: {
        repository: $osdRepository,
        ref: $osdRef,
        sha: $osdSha,
        version: $osdVersion,
        override: $osdOverride
      },
      releaseLineValidationBypassed: false
    }
    | if $sqlHeadSha == "" then . else .sql.headSha = $sqlHeadSha end' \
    >"$target_tmp"
  mv "$target_tmp" "$TARGET"
  log "Wrote local target metadata: $TARGET"
fi

SQL_SHA="$(jq -r '.sql.sha' "$TARGET")"
TARGET_BRANCH="$(jq -r '.sql.targetBranch' "$TARGET")"
SQL_VERSION="$(normalize_version "$(jq -r '.sql.version' "$TARGET")")"
OSD_SHA="$(jq -r '.osd.sha' "$TARGET")"
OSD_VERSION="$(normalize_version "$(jq -r '.osd.version' "$TARGET")")"

[[ "$SQL_SHA" == "$SQL_SHA_ACTUAL" ]] ||
  die "target SQL SHA $SQL_SHA does not match checkout $SQL_SHA_ACTUAL"
[[ "$OSD_SHA" == "$OSD_SHA_ACTUAL" ]] ||
  die "target OSD SHA $OSD_SHA does not match checkout $OSD_SHA_ACTUAL"
[[ "$SQL_VERSION" == "$SQL_VERSION_ACTUAL" ]] ||
  die "target SQL version $SQL_VERSION does not match build.gradle $SQL_VERSION_ACTUAL"
[[ "$OSD_VERSION" == "$OSD_VERSION_ACTUAL" ]] ||
  die "target OSD version $OSD_VERSION does not match package.json $OSD_VERSION_ACTUAL"

if [[ "$TARGET_BRANCH" != "main" ]]; then
  [[ "$TARGET_BRANCH" =~ ^[0-9]+\.[0-9]+$ ]] ||
    die "target branch must be main or an exact X.Y release branch: $TARGET_BRANCH"
  if [[ "$RELEASE_LINE_BYPASS" != "true" ]]; then
    [[ "$(release_line "$SQL_VERSION")" == "$TARGET_BRANCH" ]] ||
      die "SQL version $SQL_VERSION does not match target release line $TARGET_BRANCH"
    [[ "$(release_line "$OSD_VERSION")" == "$TARGET_BRANCH" ]] ||
      die "OSD version $OSD_VERSION does not match target release line $TARGET_BRANCH"
  fi
fi

[[ -f "$VALIDATOR" ]] || die "validation adapter not found: $VALIDATOR"
mkdir -p "$(dirname "$GRAMMAR_BUNDLE")" "$(dirname "$REPORT")" \
  "$(dirname "$SUMMARY")" "$(dirname "$CLUSTER_LOG")"

ADAPTER_ARGS=(
  --grammar "$GRAMMAR_BUNDLE"
  --cases "$GRAMMAR_CASES"
  --target "$TARGET"
  --osd-root "$OSD_ROOT"
  --osd-sha "$OSD_SHA"
  --report "$REPORT"
  --summary "$SUMMARY"
)

headless_module_exists() {
  [[ -f "$OSD_ROOT/$HEADLESS_MODULE" ||
    -f "$OSD_ROOT/$HEADLESS_MODULE.ts" ||
    -f "$OSD_ROOT/$HEADLESS_MODULE.js" ||
    -f "$OSD_ROOT/$HEADLESS_MODULE.mjs" ]]
}

if ! headless_module_exists; then
  log "OSD headless grammar API is unavailable; requesting a skipped report"
  node "$VALIDATOR" "${ADAPTER_ARGS[@]}"
  log "Validation skipped; report: $REPORT"
  exit 0
fi

[[ -f "$GRAMMAR_CASES" ]] || die "grammar cases not found: $GRAMMAR_CASES"
[[ -x "$SQL_ROOT/gradlew" ]] || die "Gradle wrapper is not executable: $SQL_ROOT/gradlew"

if curl --fail --silent --max-time 2 "http://127.0.0.1:9200/_cluster/health" >/dev/null 2>&1; then
  die "port 9200 already serves an OpenSearch cluster; refusing to capture from an unknown process"
fi

: >"$CLUSTER_LOG"
log "Starting candidate SQL development cluster"
./gradlew :opensearch-sql-plugin:run >"$CLUSTER_LOG" 2>&1 &
GRADLE_PID=$!

deadline=$((SECONDS + STARTUP_TIMEOUT))
while true; do
  if ! kill -0 "$GRADLE_PID" 2>/dev/null; then
    wait "$GRADLE_PID" 2>/dev/null || gradle_status=$?
    GRADLE_PID=""
    die "Gradle run exited before cluster readiness (status ${gradle_status:-0}); see $CLUSTER_LOG"
  fi
  if curl --fail --silent --show-error --connect-timeout 2 --max-time 5 \
    "http://127.0.0.1:9200/_cluster/health" >/dev/null 2>&1; then
    break
  fi
  (( SECONDS < deadline )) ||
    die "cluster did not become ready within ${STARTUP_TIMEOUT}s; see $CLUSTER_LOG"
  sleep 2
done

CAPTURE_TMP="$(mktemp "$GRAMMAR_BUNDLE.tmp.XXXXXX")"
log "Capturing GET /_plugins/_ppl/_grammar"
if ! http_status="$(
  curl --silent --show-error --connect-timeout 2 --max-time 30 \
    --output "$CAPTURE_TMP" --write-out '%{http_code}' \
    "http://127.0.0.1:9200/_plugins/_ppl/_grammar"
)"; then
  die "grammar endpoint request failed; see $CLUSTER_LOG"
fi
if [[ "$http_status" != "200" ]]; then
  mv "$CAPTURE_TMP" "$GRAMMAR_BUNDLE"
  CAPTURE_TMP=""
  die "grammar endpoint returned HTTP $http_status"
fi

jq -e '
  def nonempty_strings:
    type == "array" and length > 0 and all(.[]; type == "string" and length > 0);
  def nonempty_integers:
    type == "array" and length > 0 and all(.[]; type == "number" and floor == .);
  def sparse_names:
    type == "array" and length > 0 and
    all(.[]; . == null or type == "string") and any(.[]; . == null);
  type == "object" and
  (.bundleVersion | type == "string" and length > 0) and
  (.antlrVersion | type == "string" and length > 0) and
  (.grammarHash | type == "string" and test("^sha256:[0-9a-fA-F]{64}$")) and
  (.lexerSerializedATN | nonempty_integers) and
  (.parserSerializedATN | nonempty_integers) and
  (.lexerRuleNames | nonempty_strings) and
  (.parserRuleNames | nonempty_strings) and
  (.channelNames | nonempty_strings) and
  (.modeNames | nonempty_strings) and
  (.startRuleIndex | type == "number" and floor == . and . >= 0) and
  (.literalNames | sparse_names) and
  (.symbolicNames | sparse_names) and
  (.tokenDictionary |
    type == "object" and length > 0 and
    all(.[]; type == "number" and floor == . and . >= 0)) and
  (.ignoredTokens | type == "array" and all(.[]; type == "number" and floor == .)) and
  (.rulesToVisit | nonempty_integers)
' "$CAPTURE_TMP" >/dev/null || die "grammar endpoint returned a malformed bundle"

mv "$CAPTURE_TMP" "$GRAMMAR_BUNDLE"
CAPTURE_TMP=""
log "Captured structurally valid grammar bundle: $GRAMMAR_BUNDLE"
stop_cluster

if [[ "$SKIP_OSD_BOOTSTRAP" == "1" ]]; then
  log "Skipping OSD bootstrap because PPL_LINT_SKIP_OSD_BOOTSTRAP=1"
else
  command -v yarn >/dev/null 2>&1 || die "required command not found: yarn"
  log "Bootstrapping OSD"
  (cd "$OSD_ROOT" && yarn osd bootstrap) || die "OSD bootstrap failed"
fi

[[ -d "$OSD_ROOT/src/setup_node_env" || -f "$OSD_ROOT/src/setup_node_env.js" ||
  -f "$OSD_ROOT/src/setup_node_env.ts" || -f "$OSD_ROOT/src/setup_node_env" ]] ||
  die "OSD setup_node_env entry point not found"

log "Validating candidate grammar with OSD@$OSD_SHA"
(
  cd "$OSD_ROOT"
  node -r ./src/setup_node_env "$VALIDATOR" "${ADAPTER_ARGS[@]}"
)
log "Validation completed; report: $REPORT"

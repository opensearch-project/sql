#!/usr/bin/env bash
#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# Export the candidate SQL runtime grammar and validate it with the paired
# OpenSearch Dashboards (OSD) headless PPL linter.

set -euo pipefail

SQL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SQL_ROOT"

CANONICAL_OSD_REPOSITORY="opensearch-project/OpenSearch-Dashboards"
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
SKIP_OSD_BOOTSTRAP="${PPL_LINT_SKIP_OSD_BOOTSTRAP:-0}"

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
  --grammar FILE         Exported grammar bundle output path.
  --cases FILE           Grammar cases input path.
  --report FILE          Validation report output path.
  --summary FILE         Validation summary output path.
  -h, --help             Show this help.

Environment:
  PPL_LINT_SKIP_OSD_BOOTSTRAP=1
                         Skip OSD bootstrap. The caller must ensure generated
                         OSD targets match the exact checkout.
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

for command in git jq node; do
  command -v "$command" >/dev/null 2>&1 || die "required command not found: $command"
done
[[ "$SKIP_OSD_BOOTSTRAP" == "0" || "$SKIP_OSD_BOOTSTRAP" == "1" ]] ||
  die "PPL_LINT_SKIP_OSD_BOOTSTRAP must be 0 or 1"

GRAMMAR_BUNDLE="$(absolute_path "$GRAMMAR_BUNDLE")"
GRAMMAR_CASES="$(absolute_path "$GRAMMAR_CASES")"
REPORT="$(absolute_path "$REPORT")"
SUMMARY="$(absolute_path "$SUMMARY")"

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
      .releaseLineValidationBypassed == false)
  ' "$TARGET" >/dev/null || die "target metadata is missing required SQL/OSD fields: $TARGET"
  OSD_REPOSITORY_INPUT="$(jq -r '.osd.repository' "$TARGET")"
  OSD_REF_INPUT="$(jq -r '.osd.ref' "$TARGET")"
  OSD_CHECKOUT_REF="$(jq -r '.osd.sha' "$TARGET")"
else
  TARGET_BRANCH_INPUT="${TARGET_BRANCH_INPUT:-main}"
  OSD_REPOSITORY_INPUT="${OSD_REPOSITORY_INPUT:-$CANONICAL_OSD_REPOSITORY}"
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
  if [[ "$TARGET_BRANCH_INPUT" != "main" ]]; then
    [[ "$(release_line "$SQL_VERSION_ACTUAL")" == "$TARGET_BRANCH_INPUT" ]] ||
      die "SQL version $SQL_VERSION_ACTUAL does not match target release line $TARGET_BRANCH_INPUT"
    [[ "$(release_line "$OSD_VERSION_ACTUAL")" == "$TARGET_BRANCH_INPUT" ]] ||
      die "OSD version $OSD_VERSION_ACTUAL does not match target release line $TARGET_BRANCH_INPUT"
  fi
  OSD_OVERRIDE=false
  if [[ "$OSD_REPOSITORY_INPUT" != "$CANONICAL_OSD_REPOSITORY" ||
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
    --arg osdVersionRaw "$OSD_VERSION_RAW_ACTUAL" \
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
        versionRaw: $osdVersionRaw,
        version: $osdVersion,
        override: $osdOverride
      }
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
OSD_REF="$(jq -r '.osd.ref' "$TARGET")"
OSD_OVERRIDE="$(jq -r '.osd.override // .manualOverride // false' "$TARGET")"
OSD_VERSION="$(normalize_version "$(jq -r '.osd.version' "$TARGET")")"

[[ "$SQL_SHA" == "$SQL_SHA_ACTUAL" ]] ||
  die "target SQL SHA $SQL_SHA does not match checkout $SQL_SHA_ACTUAL"
[[ "$OSD_SHA" == "$OSD_SHA_ACTUAL" ]] ||
  die "target OSD SHA $OSD_SHA does not match checkout $OSD_SHA_ACTUAL"
[[ "$SQL_VERSION" == "$SQL_VERSION_ACTUAL" ]] ||
  die "target SQL version $SQL_VERSION does not match build.gradle $SQL_VERSION_ACTUAL"
[[ "$OSD_VERSION" == "$OSD_VERSION_ACTUAL" ]] ||
  die "target OSD version $OSD_VERSION does not match package.json $OSD_VERSION_ACTUAL"
[[ "$TARGET_BRANCH" == "main" || "$TARGET_BRANCH" =~ ^[0-9]+\.[0-9]+$ ]] ||
  die "target branch must be main or an exact X.Y release branch: $TARGET_BRANCH"
if [[ "$OSD_OVERRIDE" != "true" && "$OSD_REF" != "$TARGET_BRANCH" ]]; then
  die "target OSD ref $OSD_REF does not match target branch $TARGET_BRANCH"
fi
if [[ "$TARGET_BRANCH" != "main" ]]; then
  [[ "$(release_line "$SQL_VERSION")" == "$TARGET_BRANCH" ]] ||
    die "SQL version $SQL_VERSION does not match target release line $TARGET_BRANCH"
  [[ "$(release_line "$OSD_VERSION")" == "$TARGET_BRANCH" ]] ||
    die "OSD version $OSD_VERSION does not match target release line $TARGET_BRANCH"
fi

[[ -f "$VALIDATOR" ]] || die "validation adapter not found: $VALIDATOR"
[[ -f "$GRAMMAR_CASES" ]] || die "grammar cases not found: $GRAMMAR_CASES"
[[ -x "$SQL_ROOT/gradlew" ]] || die "Gradle wrapper is not executable: $SQL_ROOT/gradlew"
mkdir -p "$(dirname "$GRAMMAR_BUNDLE")" "$(dirname "$REPORT")" "$(dirname "$SUMMARY")"

log "Exporting candidate grammar bundle"
./gradlew :ppl:exportPplGrammarBundle --no-daemon \
  "-PpplGrammarBundleOutput=$GRAMMAR_BUNDLE"
[[ -s "$GRAMMAR_BUNDLE" ]] || die "grammar exporter did not write $GRAMMAR_BUNDLE"

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

ADAPTER_ARGS=(
  --grammar "$GRAMMAR_BUNDLE"
  --cases "$GRAMMAR_CASES"
  --target "$TARGET"
  --osd-root "$OSD_ROOT"
  --osd-sha "$OSD_SHA"
  --report "$REPORT"
  --summary "$SUMMARY"
)

log "Validating candidate grammar with OSD@$OSD_SHA"
(
  cd "$OSD_ROOT"
  node -r ./src/setup_node_env "$VALIDATOR" "${ADAPTER_ARGS[@]}"
)
log "Validation completed; report: $REPORT"

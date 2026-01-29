# Domain Checklist (core/opensearch/ppl/docs)

## Correctness
- PPL semantics: output correctness and compatibility with expected behavior.
- Calcite pushdown: verify pushdown changes do not alter results.
- Query size limits: confirm correctness vs performance expectations.
- Multi-value and nested fields: validate behavior and test coverage.

## Safety and lifecycle
- Pagination/PIT: creation/cleanup logic and fetch_size semantics.
- Exception handling: preserve root cause and avoid masking failures.
- Resource cleanup: ensure request lifecycle and cursor management are correct.

## Core/opensearch integration
- Permission checks: avoid extra privileges or unintended API usage.
- Request initialization: ensure required builders are initialized before use.

## Tests
- Unit or integration tests updated for behavior changes.
- Expected output snapshots updated when plan changes.
- CI failures or flaky tests acknowledged with reproduction info.

## Docs/doctest
- User-facing changes documented or explicitly marked as not needed.
- Doctest updates for syntax or behavior changes.

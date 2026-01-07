# Task 6 Context - UNION RECURSIVE Tests and Docs

## What changed
- Added user-facing command documentation:
  - `docs/user/ppl/cmd/union_recursive.md`
  - Added command entry in `docs/user/ppl/index.md`
- Added YAML REST integration test for recursive queries:
  - `integ-test/src/yamlRestTest/resources/rest-api-spec/test/issues/5000.yml`

## Notes/decisions
- The REST test enables Calcite, creates an `edges` index, and validates a
  `union recursive` query with `max_depth=1` for deterministic output.
- Documentation calls out Calcite requirement, schema alignment, and UNION ALL
  semantics.

## Tests run
- Not run (YAML REST tests can be executed via `./gradlew :integ-test:yamlRestTest`).

## Files touched
- `docs/user/ppl/cmd/union_recursive.md`
- `docs/user/ppl/index.md`
- `integ-test/src/yamlRestTest/resources/rest-api-spec/test/issues/5000.yml`

## Next task pointer
- Task 7: None (Task 6 completes the planned work).

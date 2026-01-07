# Task 7 Context - Refactor UNION RECURSIVE YAML REST Test

## What changed
- Updated YAML REST test data, mapping, and query to the BoM example:
  - `integ-test/src/yamlRestTest/resources/rest-api-spec/test/issues/5000.yml`

## Notes/decisions
- Uses the provided BoM mapping and bulk data.
- Uses the provided `union recursive` query and expected results.

## Tests run
- Not run (YAML REST tests can be executed via `./gradlew :integ-test:yamlRestTest`).

## Files touched
- `integ-test/src/yamlRestTest/resources/rest-api-spec/test/issues/5000.yml`
- `docs/dev/union_recursive_task7_context.md`

## Next task pointer
- None.

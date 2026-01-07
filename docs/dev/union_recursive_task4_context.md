# Task 4 Context - UNION RECURSIVE Calcite Translation

## What changed
- Added recursive relation scoping to `core/src/main/java/org/opensearch/sql/calcite/CalcitePlanContext.java`:
  - Maintains a stack of recursive relation info (name + row type).
  - Provides push/pop and lookup for recursive relations.
- Updated `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`:
  - Resolves recursive relation references to `transientScan` when in recursive scope.
  - Added `visitUnionRecursive` to build `RepeatUnion` with `max_depth` and `max_rows`.
  - Validates anchor/recursive schema alignment (field count, names, types).
  - Applies `LogicalSystemLimit` when `max_rows` is set.
- Added Calcite planner tests:
  - `ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLUnionRecursiveTest.java`.

## Notes/decisions
- `RepeatUnion` is built by pushing anchor then recursive nodes and calling
  `relBuilder.repeatUnion(relationName, true, iterationLimit)`.
- Recursive relation references are only rewritten inside the recursive block;
  outside the block, relation names resolve normally.
- Schema checks use case-insensitive name comparison and
  `SqlTypeUtil.equalSansNullability` for types.

## Tests run
- `./gradlew :ppl:test --tests org.opensearch.sql.ppl.parser.AstBuilderTest --tests org.opensearch.sql.ppl.utils.PPLQueryDataAnonymizerTest --tests org.opensearch.sql.ppl.calcite.CalcitePPLUnionRecursiveTest --tests org.opensearch.sql.ppl.antlr.PPLSyntaxParserTest`

## Files touched
- `core/src/main/java/org/opensearch/sql/calcite/CalcitePlanContext.java`
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`
- `ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLUnionRecursiveTest.java`

## Next task pointer
- Task 5: Runtime safety controls for max depth/rows enforcement and termination behavior.

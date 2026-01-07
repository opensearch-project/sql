# Task 3 Context - UNION RECURSIVE Analyzer/Resolver Rules

## What changed
- Added `UnionRecursiveValidator` to enforce recursive relation usage and scoping in
  `ppl/src/main/java/org/opensearch/sql/ppl/utils/UnionRecursiveValidator.java`.
- Wired validation into `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java` for
  `UNION RECURSIVE` subsearches.

## Validation rules
- Recursive subsearch must reference the recursive relation name at least once.
- The recursive relation name cannot be used as an alias inside the recursive block.
- The recursive relation name cannot be combined with other sources in a multi-source relation.

## Tests added/updated
- Updated `testUnionRecursive` to reference the recursive relation.
- Added negative tests for missing recursive reference, alias conflicts, and mixed sources in
  `ppl/src/test/java/org/opensearch/sql/ppl/parser/AstBuilderTest.java`.
- Updated `ppl/src/test/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizerTest.java` to
  reference the recursive relation in the subsearch.

## Tests run
- `./gradlew :ppl:test --tests org.opensearch.sql.ppl.parser.AstBuilderTest --tests org.opensearch.sql.ppl.utils.PPLQueryDataAnonymizerTest --tests org.opensearch.sql.ppl.calcite.CalcitePPLUnionRecursiveTest --tests org.opensearch.sql.ppl.antlr.PPLSyntaxParserTest`

## Files touched
- `ppl/src/main/java/org/opensearch/sql/ppl/utils/UnionRecursiveValidator.java`
- `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java`
- `ppl/src/test/java/org/opensearch/sql/ppl/parser/AstBuilderTest.java`
- `ppl/src/test/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizerTest.java`

## Next task pointer
- Task 4: Calcite translation for `UNION RECURSIVE` (RepeatUnion + recursion scoping).

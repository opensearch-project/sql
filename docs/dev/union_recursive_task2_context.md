# Task 2 Context - UNION RECURSIVE AST + Logical Plan Hooks

## What changed
- Added AST node `UnionRecursive` in `core/src/main/java/org/opensearch/sql/ast/tree/UnionRecursive.java`.
- Added AST DSL helper `unionRecursive(...)` in `core/src/main/java/org/opensearch/sql/ast/dsl/AstDSL.java`.
- Updated visitors and analyzer hooks:
  - `core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java`
  - `core/src/main/java/org/opensearch/sql/ast/EmptySourcePropagateVisitor.java`
  - `core/src/main/java/org/opensearch/sql/analysis/Analyzer.java`
- Added AST builder support to parse name/max_depth/max_rows and build `UnionRecursive` in `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java`.
- Added anonymizer support in `ppl/src/main/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizer.java`.
- Added tests:
  - `ppl/src/test/java/org/opensearch/sql/ppl/parser/AstBuilderTest.java`
  - `ppl/src/test/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizerTest.java`

## Notes/decisions
- `UnionRecursive` stores `relationName`, `maxDepth`, `maxRows`, and `recursiveSubsearch`, with the pipeline child attached separately (same pattern as `Append`).
- Name/options parsing uses `StringUtils.unquoteIdentifier` and validates:
  - name argument must be `name=<identifier>`
  - only `max_depth` and `max_rows` are accepted; duplicates are rejected
- Analyzer throws `getOnlyForCalciteException("UnionRecursive")` to keep this Calcite-only for now.

## Tests run
- `./gradlew :ppl:test --tests org.opensearch.sql.ppl.parser.AstBuilderTest --tests org.opensearch.sql.ppl.utils.PPLQueryDataAnonymizerTest`

## Files touched
- `core/src/main/java/org/opensearch/sql/ast/tree/UnionRecursive.java`
- `core/src/main/java/org/opensearch/sql/ast/dsl/AstDSL.java`
- `core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java`
- `core/src/main/java/org/opensearch/sql/ast/EmptySourcePropagateVisitor.java`
- `core/src/main/java/org/opensearch/sql/analysis/Analyzer.java`
- `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java`
- `ppl/src/main/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizer.java`
- `ppl/src/test/java/org/opensearch/sql/ppl/parser/AstBuilderTest.java`
- `ppl/src/test/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizerTest.java`

## Next task pointer
- Task 3: Analyzer/resolver rules for recursive relation scoping and name resolution inside the recursive block.

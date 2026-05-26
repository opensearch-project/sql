# New PPL Command Checklist

If you are working on contributing a new PPL command, please read this guide and review all items in the checklist are done before code review. You also can leverage this checklist to guide how to add new PPL command.

## Prerequisite

- [ ] **Open an RFC issue before starting to code:**
  - Describe the purpose of the new command
  - Include at least syntax definition, usage and examples
  - Implementation options are welcome if you have multiple ways to implement it

- [ ] **Obtain PM review approval for the RFC:**
  - If PM unavailable, consult repository maintainers as alternative
  - An offline meeting might be required to discuss the syntax and usage

## Coding & Tests

- [ ] **Lexer/Parser Updates:**
  - Add new keywords to OpenSearchPPLLexer.g4
  - Add grammar rules to OpenSearchPPLParser.g4
  - Update `commandName` and `keywordsCanBeId`

- [ ] **AST Implementation:**
  - Add new tree nodes under package `org.opensearch.sql.ast.tree`
  - Prefer reusing `Argument` for command arguments **over** creating new expression nodes under `org.opensearch.sql.ast.expression`

- [ ] **Visitor Pattern:**
  - Add `visit*` in `AbstractNodeVisitor`
  - Overriding `visit*` in `Analyzer`, `CalciteRelNodeVisitor` and `PPLQueryDataAnonymizer`

- [ ] **Unit Tests:**
  - Extend `CalcitePPLAbstractTest`
  - Keep test queries minimal
  - Include `verifyLogical()` and `verifyPPLToSparkSQL()`

- [ ] **Integration tests (pushdown):**
  - Extend `PPLIntegTestCase`
  - Use complex real-world queries
  - Include `verifySchema()` and `verifyDataRows()`

- [ ] **Integration tests (Non-pushdown):**
  - Add test class to `CalciteNoPushdownIT`

- [ ] **Explain tests:**
  - Add tests to `ExplainIT` or `CalciteExplainIT`

- [ ] **Unsupported in v2 test:**
  - Add a test in `NewAddedCommandsIT`

- [ ] **Anonymizer tests:**
  - Add a test in `PPLQueryDataAnonymizerTest`

- [ ] **Cross-cluster Tests (optional, nice to have):**
  - Add a test in `CrossClusterSearchIT`, or in `CalciteCrossClusterSearchIT` if the command requires Calcite.

- [ ] **User doc:**
  - Add a xxx.md under `docs/user/ppl/cmd` and link the new doc to `docs/user/ppl/index.md`

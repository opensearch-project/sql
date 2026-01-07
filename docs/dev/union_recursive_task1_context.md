# Task 1 Context - UNION RECURSIVE Syntax/Grammar

## What changed
- Added `UNION RECURSIVE` command grammar and recursive subpipeline rule in `ppl/src/main/antlr/OpenSearchPPLParser.g4`.
- Added lexer tokens `UNION` and `RECURSIVE` in `ppl/src/main/antlr/OpenSearchPPLLexer.g4`.
- Added parser tests (pass + fail) in `ppl/src/test/java/org/opensearch/sql/ppl/antlr/PPLSyntaxParserTest.java`.

## Notes/decisions
- The recursive block is parsed as `recursiveSubPipeline : PIPE? subSearch` so it can include an optional leading pipe.
- The name/options use generic `ident EQUAL ...` pairs in grammar for now; AST work should interpret these as `name`, `max_depth`, `max_rows` with validation in Task 2+.
- Changes in `language-grammar/` were reverted per request; only `ppl/` grammar is updated.

## Tests run
- `./gradlew :ppl:test --tests org.opensearch.sql.ppl.antlr.PPLSyntaxParserTest`

## Files touched
- `ppl/src/main/antlr/OpenSearchPPLParser.g4`
- `ppl/src/main/antlr/OpenSearchPPLLexer.g4`
- `ppl/src/test/java/org/opensearch/sql/ppl/antlr/PPLSyntaxParserTest.java`

## Next task pointer
- Task 2: Add AST and logical plan nodes for UNION RECURSIVE, including parsing of `name`, `max_depth`, and `max_rows` options from the grammar output.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenSearch SQL plugin — enables SQL and PPL (Piped Processing Language) queries against OpenSearch. This is a multi-module Gradle project (Java 21) that functions as an OpenSearch plugin.

## Build Commands

```bash
# Full build (compiles, tests, checks)
./gradlew build

# Fast build (skip integration tests)
./gradlew build -x integTest

# Build specific module
./gradlew :core:build
./gradlew :sql:build
./gradlew :ppl:build

# Run unit tests only
./gradlew test

# Run a single unit test class
./gradlew :core:test --tests "org.opensearch.sql.analysis.AnalyzerTest"

# Run integration tests
./gradlew :integ-test:integTest

# Run a single integration test
./gradlew :integ-test:integTest -Dtests.class="*QueryIT"

# Skip Prometheus if unavailable
./gradlew :integ-test:integTest -DignorePrometheus

# Code formatting
./gradlew spotlessCheck    # Check
./gradlew spotlessApply    # Auto-fix

# Regenerate ANTLR parsers from grammar files
./gradlew generateGrammarSource

# Run plugin locally with OpenSearch
./gradlew :opensearch-sql-plugin:run
./gradlew :opensearch-sql-plugin:run -DdebugJVM   # With remote debug on port 5005

# Run doctests
./gradlew :doctest:doctest
./gradlew :doctest:doctest -Pdocs=search    # Single file
```

## Code Style

- **Google Java Format** enforced via Spotless (2-space indent, 100 char line limit)
- **Lombok** is used throughout — `@Getter`, `@Builder`, `@RequiredArgsConstructor`, etc.
- **License header** required on all Java files (Apache 2.0). Missing headers fail the build.
- Pre-commit hooks run `spotlessApply` automatically
- All commits must include a DCO sign-off: `Signed-off-by: Name <email>` (use `git commit -s`).

## Architecture

### Query Pipeline

```
User Query (SQL/PPL)
  → Parsing (ANTLR) — produces parse tree
  → AST Construction (AstBuilder visitor) — produces UnresolvedPlan
  → Semantic Analysis (Analyzer) — resolves symbols/types → LogicalPlan
  → Planning (Planner + LogicalPlanOptimizer) — produces PhysicalPlan
  → Execution (ExecutionEngine) — streams ExprValue results
  → Response Formatting (ResponseFormatter — JSON/CSV/JDBC)
```

### Module Dependency Graph

```
plugin (OpenSearch plugin entry point, Guice DI wiring)
  ├── sql          — SQL parsing (ANTLR → AST via SQLSyntaxParser/AstBuilder)
  ├── ppl          — PPL parsing (ANTLR → AST via PPLSyntaxParser/AstBuilder)
  ├── core         — Central module: Analyzer, Planner, ExecutionEngine interfaces,
  │                  AST/LogicalPlan/PhysicalPlan node types, expression system, type system
  ├── opensearch   — OpenSearch storage engine, execution engine, client
  ├── protocol     — Response formatters (JSON, CSV, JDBC, YAML)
  ├── common       — Shared settings and utilities
  ├── legacy       — V1 SQL engine (backward compatibility fallback)
  ├── datasources  — Multi-datasource support (Glue, Security Lake, Prometheus)
  ├── async-query / async-query-core — Spark-based async query execution
  ├── direct-query / direct-query-core — Direct external datasource queries
  └── language-grammar — Centralized ANTLR .g4 grammar files
```

`core` has no dependency on other modules. `sql` and `ppl` depend on `core` and `language-grammar`. `opensearch` implements `core` interfaces.

### Key Source Locations

| Area | Key Files |
|------|-----------|
| Plugin entry | `plugin/.../SQLPlugin.java`, `plugin/.../OpenSearchPluginModule.java` |
| SQL parsing | `sql/.../sql/parser/AstBuilder.java`, `sql/.../SQLService.java` |
| PPL parsing | `ppl/.../ppl/parser/AstBuilder.java`, `ppl/.../PPLService.java` |
| ANTLR grammars | `language-grammar/src/main/antlr4/` (OpenSearchSQLParser.g4, OpenSearchPPLParser.g4) |
| Analysis | `core/.../analysis/Analyzer.java`, `core/.../analysis/ExpressionAnalyzer.java` |
| Planning | `core/.../planner/Planner.java`, `core/.../planner/logical/LogicalPlan.java` |
| Execution | `core/.../executor/ExecutionEngine.java`, `opensearch/.../OpenSearchExecutionEngine.java` |
| Storage | `opensearch/.../storage/OpenSearchStorageEngine.java` |
| Query orchestration | `core/.../executor/QueryService.java`, `core/.../executor/QueryPlanFactory.java` |

### Core Abstractions

- **`Node<T>`** — Base AST node with visitor pattern support
- **`UnresolvedPlan`** / **`LogicalPlan`** / **`PhysicalPlan`** — Query plan hierarchy (unresolved → logical → physical)
- **`Expression`** — Resolved expression with `valueOf()` and `type()`
- **`ExprValue`** — Runtime value types (ExprIntegerValue, ExprStringValue, etc.)
- **`ExprType`** — Type system (DATE, TIMESTAMP, DOUBLE, STRUCT, etc.)
- **`StorageEngine`** / **`Table`** — Pluggable storage abstraction
- **`ExecutionEngine`** — Executes physical plans, returns QueryResponse

### Design Patterns

- **Visitor pattern** used pervasively: `AbstractNodeVisitor`, `LogicalPlanNodeVisitor`, `PhysicalPlanNodeVisitor`, `ExpressionNodeVisitor`
- **PhysicalPlan** implements `Iterator<ExprValue>` for streaming execution
- **Guice** dependency injection in `OpenSearchPluginModule`

## Fixing PPL Bugs

Use the `/ppl-bugfix #<issue_number>` slash command to fix PPL bugs. It dispatches a subagent with scoped permissions that follows all phases in `ppl-bugfix-harness.md` — from Phase 0 (Triage & Classification) through Phase 4 (Retrospective & Improvement). The full harness content is inlined into the subagent prompt to guarantee compliance. If a user asks to fix a PPL bug without using the slash command, remind them to use `/ppl-bugfix` or invoke it on their behalf.

## Adding New PPL Commands

Follow the checklist in `docs/dev/ppl-commands.md`:
1. Update lexer/parser grammars (OpenSearchPPLLexer.g4, OpenSearchPPLParser.g4)
2. Add AST node under `org.opensearch.sql.ast.tree`
3. Add `visit*` method in `AbstractNodeVisitor`, override in `Analyzer`, `CalciteRelNodeVisitor`, `PPLQueryDataAnonymizer`
4. Unit tests extending `CalcitePPLAbstractTest` (include `verifyLogical()` and `verifyPPLToSparkSQL()`)
5. Integration tests extending `PPLIntegTestCase`
6. Add user docs under `docs/user/ppl/cmd/`

## Adding New PPL Functions

Follow `docs/dev/ppl-functions.md`. Three approaches:
1. Reuse existing Calcite operators from `SqlStdOperatorTable`/`SqlLibraryOperators`
2. Adapt static Java methods via `UserDefinedFunctionUtils.adapt*ToUDF`
3. Implement `ImplementorUDF` interface from scratch, register in `PPLBuiltinOperators`

## Calcite Engine

The project has two execution engines: the legacy **v2 engine** and the newer **Calcite engine** (Apache Calcite-based). Calcite is toggled via `plugins.calcite.enabled` setting (default: off in production, toggled per-test in integration tests).

- In integration tests, call `enableCalcite()` in `init()` to activate the Calcite path
- Some features (e.g., graphLookup) require pushdown optimization — use `enabledOnlyWhenPushdownIsEnabled()` to skip tests in the `CalciteNoPushdownIT` suite
- `CalciteNoPushdownIT` is a JUnit `@Suite` that re-runs Calcite test classes with pushdown disabled; add new test classes to its `@Suite.SuiteClasses` list

## Integration Tests

Located in `integ-test/src/test/java/`. Organized by area: `sql/`, `ppl/`, `calcite/`, `legacy/`, `jdbc/`, `datasource/`, `asyncquery/`, `security/`. Uses OpenSearch test framework (in-memory cluster per test class). YAML REST tests in `integ-test/src/yamlRestTest/resources/rest-api-spec/test/`.

Key base classes:
- `PPLIntegTestCase` — base for PPL integration tests (v2 engine)
- `CalcitePPLIT` — base for Calcite PPL integration tests (calls `enableCalcite()`)
- `CalcitePPLAbstractTest` — base for Calcite PPL unit tests (`verifyLogical()`, `verifyPPLToSparkSQL()`)
- `CalciteExplainIT` — explain plan tests using YAML expected output files in `integ-test/src/test/resources/expectedOutput/calcite/`

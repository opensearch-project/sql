# PPL Bugfix Reference

Consult this file when you need fix-path-specific guidance or test templates.

---

## Fix Path Reference

### Path A — Grammar / Parser

1. Update grammar files (must stay in sync):
   - `language-grammar/src/main/antlr4/OpenSearchPPLParser.g4` (primary)
   - `ppl/src/main/antlr/OpenSearchPPLParser.g4`
   - `async-query-core/src/main/antlr/OpenSearchPPLParser.g4` (if applicable)
2. Regenerate: `./gradlew generateGrammarSource`
3. Update AstBuilder: `ppl/.../parser/AstBuilder.java`
4. Test: `AstBuilderTest`

### Path B — AST / Function Implementation

1. AST nodes in `core/.../ast/tree/`, functions in `core/.../expression/function/` or `PPLBuiltinOperators`
2. Watch Visitor pattern — sync `AbstractNodeVisitor`, `Analyzer`, `CalciteRelNodeVisitor`, `PPLQueryDataAnonymizer`
3. Test: `verifyLogical()`, `verifyPPLToSparkSQL()`, `verifyResult()`

### Path C — Type System / Semantic Analysis

1. `OpenSearchTypeFactory.java`, `Analyzer.java`, `ExpressionAnalyzer.java`
2. Preserve nullable semantics; protect UDT from `leastRestrictive()` downgrade
3. Test: type preservation, nullable propagation, mixed types

### Path D — Optimizer / Predicate Pushdown

1. `PredicateAnalyzer.java`, `LogicalPlanOptimizer`, `QueryService.java`
2. Watch `nullAs` semantics; for plan bloat consider `FilterMergeRule`
3. Verify: `EXPLAIN` output + integration test correctness

### Path E — Execution / Resource Management

1. `OpenSearchExecutionEngine.java`, `SQLPlugin.java`, `OpenSearchPluginModule.java`
2. Common patterns: cache key collision, memory leak, unbounded growth, non-singleton, DI not injected

---

## Test Templates

**Unit test** (extend `CalcitePPLAbstractTest`):
```java
public class CalcitePPLYourFixTest extends CalcitePPLAbstractTest {
    public CalcitePPLYourFixTest() {
        super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
    }

    @Before
    public void init() {
        doReturn(true).when(settings)
            .getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    }

    @Test
    public void testBugScenario() {
        verifyLogical("source=EMP | where SAL > 1000",
            "LogicalFilter(condition=[>($5, 1000)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    }
}
```

**Integration test** (extend `CalcitePPLIT`):
```java
public class CalcitePPLYourFixIT extends CalcitePPLIT {
    @Override
    public void init() throws IOException {
        super.init();
        enableCalcite();
    }

    @Test
    public void testBugFixEndToEnd() throws IOException {
        JSONObject result = executeQuery("source=<index> | <your PPL>");
        verifySchema(result, schema("field", "alias", "type"));
        verifyDataRows(result, rows("expected_value_1"), rows("expected_value_2"));
    }
}
```

**YAML REST test** — place at `integ-test/src/yamlRestTest/resources/rest-api-spec/test/issues/<ISSUE>.yml`:
```yaml
setup:
  - do:
      indices.create:
        index: test_issue_<ISSUE>
        body:
          settings: { number_of_shards: 1, number_of_replicas: 0 }
          mappings: { properties: { <field>: { type: <type> } } }
  - do:
      query.settings:
        body: { transient: { plugins.calcite.enabled: true } }
---
teardown:
  - do:
      query.settings:
        body: { transient: { plugins.calcite.enabled: false } }
---
"<Bug description for issue #ISSUE>":
  - skip: { features: [headers, allowed_warnings] }
  - do:
      bulk: { index: test_issue_<ISSUE>, refresh: true, body: ['{"index": {}}', '{"<field>": "<value>"}'] }
  - do:
      headers: { Content-Type: 'application/json' }
      ppl: { body: { query: "source=test_issue_<ISSUE> | <your PPL>" } }
  - match: { total: <expected> }
  - match: { datarows: [ [ <row1_val1>, <row1_val2> ], [ <row2_val1>, <row2_val2> ] ] }
```

> **Always include `datarows` assertions** — verifying only `total` and `schema` will miss
> wrong values. Count the expected output groups carefully (e.g., for `chart ... by <col>`,
> count distinct (row_split, col_split) groups after null filtering, not the number of input rows).

---

## Symptom → Fix Path

```
SyntaxCheckException / unrecognized syntax       → Path A
SemanticCheckException / type mismatch            → Path C
Field type wrong (timestamp→string)               → Path C
EXPLAIN shows predicate not pushed down           → Path D
Multi-condition query: missing/extra rows         → Path D
OOM / memory growth over time                     → Path E
NPE in Transport layer                            → Path E
"node must be boolean/number, found XXX"          → Path B
Regex/function extraction offset                  → Path B
```

---

## Case Index

| Commit | Bug | Layer | Tests |
|--------|-----|-------|-------|
| `ada2e34` | UNION loses UDT type | Type System | 8 UT + 4 IT |
| `26674f9` | rex capture group index shift | AST/Functions | Multiple UTs |
| `b4df010` | isnotnull not pushed down with != | Optimizer | 2 UT + IT |
| `e045d15` | Multiple filters OOM | Optimizer | 26 output updates |
| `f024b4f` | High-cardinality GROUP BY OOM | Execution | Benchmark |
| `97d5d26` | OrdinalMap cache collision + leak | Execution | — |
| `90393bf` | Non-singleton ExecutionEngine leak | Resource | — |
| `f6be830` | Transport extensions not injected | DI | — |
| `734394d` | Grammar rule typo | Grammar | — |
| `246ed0d` | Float precision flaky test | Test Infra | — |
| `d56b8fa` | Wildcard index type conflict | Value Parsing | 3 UT + 1 IT + 1 YAML |
| `5a78b78` | Boolean coercion from numeric in wildcard queries | Value Parsing | 3 UT + 1 IT + 1 YAML |

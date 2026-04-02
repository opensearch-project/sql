# PPL Bugfix Harness

> Systematic bugfix workflow distilled from 15+ historical commits. Invoke via `/ppl-bugfix #<issue_number>`.

---

## Phase 0: Triage & Classification

### 0.1 Reproduce the Bug

```bash
# Minimal PPL query
POST /_plugins/_ppl/_query
{ "query": "<your PPL query here>" }

# Or via integration test
./gradlew :integ-test:integTest -Dtests.class="*<TestClass>" -Dtests.method="<testMethod>"
```

**If not reproducible on latest `main`**:

| Finding | Action |
|---------|--------|
| Already fixed by another commit | Comment with fixing commit hash, close as duplicate |
| Only on older version | Comment with version where it's fixed, close as resolved |
| Intermittent / environment-specific | Label `flaky` or `needs-info`, do NOT close |
| Insufficient info to reproduce | Comment asking for repro steps, label `needs-info` |

In all cases, **stop and report back** — do not proceed to Phase 1.

### 0.2 Classify and Route

Identify the bug layer, then use the routing table to determine the fix path and required tests:

| Layer | Typical Symptoms | Fix Path | Required Tests |
|-------|-----------------|----------|---------------|
| **Grammar/Parser** | `SyntaxCheckException`, unrecognized syntax | Path A | AstBuilderTest |
| **AST/Functions** | Parses OK but AST wrong, function output wrong | Path B | CalcitePPL\*Test + IT + YAML |
| **Semantic Analysis** | `SemanticCheckException`, type mismatch | Path C | Dedicated UT + CalcitePPLIT + YAML |
| **Type System** | Field type lost, implicit conversion errors | Path C | Dedicated UT + CalcitePPLIT + YAML |
| **Optimizer** | Plan bloat, predicate pushdown failure | Path D | CalcitePPL\*Test + CalcitePPLIT + NoPushdownIT + YAML |
| **Execution** | Wrong results, OOM, memory leaks | Path E | IT + YAML |
| **DI / Resource** | NPE, extensions not loaded, long-running OOM | Path E | IT + YAML |

Record before proceeding:

```
Bug Layer: <from table above>
Fix Path:  <A / B / C / D / E>
Issue:     #XXXX
```

### 0.3 Execution Flow

Phases interleave TDD-style — write a failing test first, then fix, then complete test coverage:

```
Phase 0: Triage → Classify → Route
  → Phase 2 (partial): Write FAILING test reproducing the bug
  → Phase 1: Implement fix via routed Path
  → Phase 2 (remaining): Happy path, edge cases, YAML REST test
  → Phase 3: Verify → Commit → PR → Decision Log
```

---

## Phase 1: Fix Implementation

### Path A — Grammar / Parser

1. **Update grammar files** (must stay in sync):
   - `language-grammar/src/main/antlr4/OpenSearchPPLParser.g4` (primary)
   - `ppl/src/main/antlr/OpenSearchPPLParser.g4`
   - `async-query-core/src/main/antlr/OpenSearchPPLParser.g4` (if applicable)
2. **Regenerate**: `./gradlew generateGrammarSource`
3. **Update AstBuilder**: `ppl/.../parser/AstBuilder.java` — modify `visit*()` to match new rule
4. **Test**: `AstBuilderTest` using `assertEqual(pplQuery, expectedAstNode)` pattern

> Ref: `734394d` — fixed `renameClasue` → `renameClause` across 3 grammars + AstBuilder

### Path B — AST / Function Implementation

1. **Locate**: AST nodes in `core/.../ast/tree/`, functions in `core/.../expression/function/` or `PPLBuiltinOperators`
2. **Fix**: Watch Visitor pattern — changes may need syncing to `AbstractNodeVisitor`, `Analyzer`, `CalciteRelNodeVisitor`, `PPLQueryDataAnonymizer`
3. **Test**: `verifyLogical()`, `verifyPPLToSparkSQL()`, `verifyResult()`

> Ref: `26674f9` — rex nested capture group fix, ordinal index → named group extraction

### Path C — Type System / Semantic Analysis

1. **Locate**: `OpenSearchTypeFactory.java` (Calcite type factory), `Analyzer.java` / `ExpressionAnalyzer.java`
2. **Fix**: Preserve nullable semantics when overriding Calcite methods; protect UDT from `leastRestrictive()` downgrade
3. **Test (critical)**: Cover type preservation, nullable propagation, fallback to parent, mixed types — every edge case

> Ref: `ada2e34` — UNION lost timestamp UDT, fixed `leastRestrictive()`, added 8 UTs + 4 ITs

### Path D — Optimizer / Predicate Pushdown

1. **Locate**: `PredicateAnalyzer.java`, `LogicalPlanOptimizer`, `QueryService.java`
2. **Fix**: Watch `nullAs` semantics (TRUE/FALSE/UNKNOWN); for plan bloat consider Calcite rules like `FilterMergeRule`
3. **Verify**: `EXPLAIN` output comparison + integration test result correctness

> Ref: `b4df010` — `isnotnull()` not pushed down with multiple `!=`; `e045d15` — multi-filter OOM, inserted `FilterMergeRule`

### Path E — Execution / Resource Management

1. **Locate**: DQE operators in `dqe/`, `OpenSearchExecutionEngine.java`, `SQLPlugin.java`, `OpenSearchPluginModule.java`
2. **Common patterns**:

   | Problem | Fix | Example |
   |---------|-----|---------|
   | Cache key collision | `IndexReader.CacheHelper.getKey()` | `97d5d26` |
   | Memory leak (no eviction) | Close listener + upper bound | `97d5d26` |
   | Unbounded growth | `MAX_CAPACITY` check, throw with user guidance | `f024b4f` |
   | Non-singleton repeated registration | `@Singleton`; `put` instead of `compute/append` | `90393bf` |
   | DI not injected | Holder class → Guice → constructor injection | `f6be830` |

> Ref: `f024b4f` — `LongOpenHashSet` capacity 1024→8 (8KB→64B per group), 8M cap on 5 HashMap variants

---

## Phase 2: Writing Tests

### 2.1 Test Templates

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

**YAML REST test** — place at `integ-test/src/yamlRestTest/resources/rest-api-spec/test/issues/<ISSUE>.yml`, auto-discovered by `RestHandlerClientYamlTestSuiteIT`:
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
  - length: { datarows: <expected> }
```

### 2.2 Test Checklist

- [ ] Failing test reproducing the original bug (write FIRST, before fixing)
- [ ] Happy path after fix
- [ ] Edge cases (null, empty, extreme volumes)
- [ ] YAML REST test (named by issue number)
- [ ] Optimizer bugs: both pushdown enabled and disabled
- [ ] Type system bugs: nullable / non-nullable combinations
- [ ] New AST nodes: update `PPLQueryDataAnonymizerTest`

---

## Phase 3: Verification & Submission

### 3.1 Local Verification

```bash
./gradlew spotlessApply                                      # 1. Format
./gradlew :<module>:test --tests "<TestClass>"               # 2. Affected module UT
./gradlew test                                               # 3. Full UT regression
./gradlew :integ-test:integTest -Dtests.class="*<YourIT>"   # 4. New IT
./gradlew :integ-test:integTest                              # 5. Full IT regression
./gradlew :integ-test:yamlRestTest                           # 6. YAML REST tests
# If grammar modified:
./gradlew generateGrammarSource && ./gradlew :ppl:test       # 7. Parser tests
```

### 3.2 Commit & Push

The subagent runs in an isolated **git worktree** branched from HEAD. All changes are on a dedicated branch that does not affect the user's working directory.

```bash
# Commit with DCO sign-off
git add <changed_files>
git commit -s -m "[BugFix] Fix <description> (#<issue_number>)"

# Sync with main (merge, not rebase — PRs use squash-merge)
git fetch origin && git merge origin/main
# Re-run tests if merge brought changes

# Push to personal fork (NOT origin if it points to upstream)
git remote -v  # confirm your fork remote
git push -u <your_fork_remote> <branch_name>
```

### 3.3 Create Draft PR

```bash
gh pr create --draft --title "[BugFix] Fix <description> (#<issue_number>)" --body "$(cat <<'EOF'
### Description
<Brief description of fix and root cause>

### Related Issues
Resolves #<issue_number>

### Check List
- [x] New functionality includes testing
- [x] Javadoc added for new public API
- [x] Commits signed per DCO (`-s`)
- [x] `spotlessCheck` passed
- [x] Unit tests passed
- [x] Integration tests passed
- [ ] Grammar changes: all three `.g4` files synced (if applicable)
EOF
)"
```

### 3.4 Persist Decision Context

Before the subagent completes, persist the *why* behind key decisions — the PR diff only shows the *what*. The **PR comment is the single source of truth** — it survives across sessions, GA runs, and is visible to both human reviewers and follow-up agents.

```bash
gh pr comment <pr_number> --body "$(cat <<'EOF'
## Decision Log
**Root Cause**: <what actually caused the bug>
**Approach**: <what was implemented and why>
**Alternatives Rejected**: <what was considered and why it didn't work>
**Pitfalls**: <dead ends, subtle edge cases discovered>
**Things to Watch**: <known limitations, areas needing follow-up>
EOF
)"
```

---

## Phase 3.5: Post-PR Follow-up

Runs as a **separate subagent** invocation — the original agent has completed. Invoke via `/ppl-bugfix #<issue>` or `/ppl-bugfix PR#<pr>` (auto-detects follow-up mode when a PR exists).

### 3.5.1 Reconstruct Context

The follow-up agent runs in a fresh worktree. First checkout the PR branch, then load state:

```bash
# Checkout the PR branch in this worktree
gh pr checkout <pr_number>

# Load PR state and Decision Log
gh pr view <pr_number> --json title,body,state,reviews,comments,statusCheckRollup,mergeable
gh pr checks <pr_number>
# Read the Decision Log comment FIRST — contains rejected alternatives and pitfalls
gh api repos/<owner>/<repo>/pulls/<pr_number>/comments
```

### 3.5.2 Handle Review Feedback

For each comment, **cross-check against the Decision Log first**:

| Type | Action |
|------|--------|
| Code change | If already rejected in Decision Log, reply with reasoning. Otherwise make the change, new commit, push |
| Question | Reply with explanation — Decision Log often has the answer |
| Nit | Fix if trivial |
| Disagreement | Reply with Decision Log reasoning; if reviewer insists, escalate to user |

```bash
git add <files> && git commit -s -m "Address review feedback: <description>"
git push <your_fork_remote> <branch_name>
```

### 3.5.3 Handle CI Failures

```bash
gh pr checks <pr_number>                  # Identify failures
gh run view <run_id> --log-failed         # Read logs
# Test failure → fix locally, push new commit
# Spotless → ./gradlew spotlessApply, push
# Flaky → gh run rerun <run_id> --failed
```

### 3.5.4 Handle Merge Conflicts

```bash
git fetch origin && git merge origin/main  # Resolve conflicts
./gradlew spotlessApply && ./gradlew test && ./gradlew :integ-test:integTest  # Re-verify
git commit -s -m "Resolve merge conflicts with main"
git push <your_fork_remote> <branch_name>
```

### 3.5.5 Mark Ready

```bash
gh pr ready <pr_number>
```

---

## Phase 4: Retrospective

After each bugfix, check if the harness itself needs updating:

- **Template wrong** (API name, assertion field)? → Fix in Phase 2 templates
- **New bug pattern** not covered? → Add fix path in Phase 1 or symptom in Quick Reference
- **Verification gap** caused rework? → Add step to 3.1 or 2.2 checklist
- **Representative fix**? → Add row to Case Index below

---

## Quick Reference: Symptom → Fix Path

```
SyntaxCheckException / unrecognized syntax       → Path A: Grammar/Parser
SemanticCheckException / type mismatch            → Path C: Type System / Analysis
Field type wrong (timestamp→string)               → Path C: check leastRestrictive / coercion
EXPLAIN shows predicate not pushed down           → Path D: Optimizer / Pushdown
Multi-condition query: missing/extra rows         → Path D: PredicateAnalyzer nullAs handling
OOM / memory growth over time                     → Path E: singletons, cache eviction, bounds
NPE in Transport layer                            → Path E: DI / Guice injection chain
"node must be boolean/number, found XXX"          → Path B: OpenSearchJsonContent parse*Value
Regex/function extraction offset                  → Path B: ordinal vs named references
```

---

## Appendix: Case Index

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

# PPL Bugfix Harness

## Phase 0: Triage

### 0.0 Report Working Directory

```bash
echo "Worktree: $(pwd)"
echo "Branch: $(git branch --show-current)"
```

Include this in your output so the caller knows where changes are happening.

### 0.1 Load & Reproduce

```bash
gh issue view <issue_number> --repo opensearch-project/sql
```

Write a failing test or run an existing one to reproduce the bug on `main`.

If the bug **does not reproduce** (correct results, not infra failure):

| Finding | Action |
|---------|--------|
| Already fixed | `gh issue comment` + `gh issue close` |
| Older version only | `gh issue comment` + `gh issue close` |
| Intermittent | Label `flaky` or `needs-info`, do NOT close |
| Can't reproduce | Comment asking for repro steps, label `needs-info` |

**HARD STOP** — do not proceed. Report back.

### 0.2 Classify

Identify the bug layer (Grammar, AST/Functions, Type System, Optimizer, Execution, DI/Resource) and record it. Consult `.claude/harness/ppl-bugfix-reference.md` for fix-path-specific guidance if needed.

### 0.3 Guardrails

Stop and report back if:
- Root cause unclear after reading 15+ source files
- Fix breaks 5+ unrelated tests
- Same build error 3 times in a row

### 0.4 Execution Flow

```
Triage → Write FAILING test → Fix → Remaining tests → Verify → Commit → PR → Decision Log → Completion Gate
```

---

## Phase 1: Fix

Find and fix the root cause. Consult `.claude/harness/ppl-bugfix-reference.md` for path-specific patterns and examples.

---

## Phase 2: Tests

Consult `.claude/harness/ppl-bugfix-reference.md` for test templates.

Required deliverables:
- Failing test reproducing the bug (written BEFORE the fix)
- Unit tests covering happy path and edge cases
- Integration test — add to an existing `*IT.java` when possible; if creating a new one, add it to `CalciteNoPushdownIT`
- YAML REST test at `integ-test/src/yamlRestTest/resources/rest-api-spec/test/issues/<ISSUE>.yml`

---

## Phase 3: Verify & Submit

### 3.1 Verify

```bash
./gradlew spotlessApply
./gradlew :<module>:test --tests "<TestClass>"
./gradlew test
./gradlew :integ-test:integTest -Dtests.class="*<YourIT>"
```

Run `./gradlew :integ-test:yamlRestTest` if YAML tests were added. Run `./gradlew generateGrammarSource && ./gradlew :ppl:test` if grammar was modified.

### 3.2 Commit & PR

```bash
git add <changed_files>
git commit -s -m "[BugFix] Fix <description> (#<issue_number>)"
git fetch origin && git merge origin/main
./gradlew test && ./gradlew :integ-test:integTest -Dtests.class="*<YourIT>"

# Resolve fork remote (check git remote -v; add if missing)
git remote add fork https://github.com/<fork_owner>/sql.git
git push -u fork <branch_name>
```

Do NOT add Co-Authored-By lines. Use the git user name to infer the fork owner, or fall back to "qianheng-aws".

```bash
gh pr create --draft --repo opensearch-project/sql \
  --title "[BugFix] Fix <description> (#<issue_number>)" \
  --body "$(cat <<'EOF'
### Description
<Brief description of fix and root cause>

### Related Issues
Resolves #<issue_number>

### Check List
- [x] New functionality includes testing
- [x] Commits signed per DCO (`-s`)
- [x] `spotlessCheck` passed
- [x] Unit tests passed
- [x] Integration tests passed
EOF
)"
```

### 3.3 Decision Log

Post as a PR comment:

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

## Completion Gate

Run `git status --porcelain` — if any uncommitted changes remain, commit and push them before proceeding.

Do NOT report "done" until every item below is checked. List each in your final report:

- [ ] **Unit tests**: New test class or methods
- [ ] **Integration test**: New `*IT.java` test
- [ ] **YAML REST test**: `issues/<ISSUE>.yml`
- [ ] **spotlessApply**: Ran successfully
- [ ] **Tests pass**: Affected modules
- [ ] **Commit**: DCO sign-off, `[BugFix]` prefix, no Co-Authored-By
- [ ] **Draft PR**: `--draft`, body contains `Resolves #<issue>`
- [ ] **Decision Log**: PR comment posted

If any item is blocked, report which and why.

---

## Phase 4: Retrospective

- [ ] Symptom in Quick Reference? Add if missing.
- [ ] Classification correct? Fix routing if misleading.
- [ ] Test template worked as-is? Fix if broken.
- [ ] New pattern? Add to Case Index.

Include harness improvements in the same PR.

Report in your final output:
```
Worktree: <absolute path>
Branch: <branch name>
PR: <pr_number>
```

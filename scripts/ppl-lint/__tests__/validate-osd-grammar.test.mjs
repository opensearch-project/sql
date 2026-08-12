/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

const SCRIPT = fileURLToPath(new URL('../validate-osd-grammar.mjs', import.meta.url));
const CORPUS = fileURLToPath(new URL('../grammar-cases.json', import.meta.url));
const HASH = `sha256:${'a'.repeat(64)}`;
const HEADLESS_BASE = path.join(
  'src',
  'plugins',
  'data',
  'public',
  'antlr',
  'opensearch_ppl',
  'headless_ppl_lint'
);
const CATALOG_BASE = path.join(
  'packages',
  'osd-monaco',
  'src',
  'ppl',
  'lint',
  'catalog'
);

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(value));
}

function writeText(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, value);
}

function makeTarget({
  branch = '3.8',
  sqlVersion = '3.8.0',
  osdVersion = '3.8.1',
  osdRef = branch,
  releaseLineValidationBypassed = false,
} = {}) {
  return {
    sql: {
      sha: 'sql-tested-sha',
      headSha: 'sql-head-sha',
      targetBranch: branch,
      versionRaw: `${sqlVersion}-SNAPSHOT`,
      version: sqlVersion,
    },
    osd: {
      repository: 'opensearch-project/OpenSearch-Dashboards',
      ref: osdRef,
      version: osdVersion,
    },
    releaseLineValidationBypassed,
  };
}

function exclusion(ruleId, reason = `Reason for excluding ${ruleId}.`) {
  return { ruleId, reason };
}

function defaultCases(ruleIds = ['rule-a'], excludedRules = []) {
  return {
    schemaVersion: 2,
    excludedRules,
    cases: ruleIds.flatMap((ruleId) => [
      {
        id: `${ruleId}-trigger`,
        ruleId,
        kind: 'trigger',
        query: `source=accounts | ${ruleId} trigger`,
        expectedCount: 1,
        context: { isCalcite: true },
      },
      {
        id: `${ruleId}-control`,
        ruleId,
        kind: 'control',
        query: `source=accounts | ${ruleId} control`,
        expectedCount: 0,
        context: { isCalcite: true },
      },
    ]),
  };
}

function headlessSource({ format = 'cjs', missingExport = false, includeRecoveredTree = false } = {}) {
  const declarations = `
const deserializeBundleOrThrow = (bundle) => ({ grammarHash: bundle.grammarHash });
${includeRecoveredTree ? `
const buildRuntimeTree = () => ({
  tree: { children: [{ constructor: { name: 'ErrorNode' } }] }
});` : ''}
${missingExport ? '' : `
const lintQueryWithBundle = (query, grammar, context) => {
  if (query.includes('throws')) throw new Error('detector crashed');
  const target = Object.entries(context.overrides)
    .find(([, override]) => override.enabled)?.[0];
  const diagnostics = [];
  if (query.includes('trigger') || query.includes('target-diagnostic')) {
    diagnostics.push({ ruleId: target });
  }
  if (query.includes('wrong-rule')) diagnostics.push({ ruleId: 'some-other-rule' });
  if (query.includes('missing-rule-id')) diagnostics.push({});
  return { diagnostics };
};`}
`;
  const names = [
    'deserializeBundleOrThrow',
    ...(includeRecoveredTree ? ['buildRuntimeTree'] : []),
    ...(missingExport ? [] : ['lintQueryWithBundle']),
  ];
  if (format === 'esm') {
    return `${declarations}\nexport { ${names.join(', ')} };\n`;
  }
  return `${declarations}\n${names.map((name) => `exports.${name} = ${name};`).join('\n')}\n`;
}

function headlessPath(osdRoot, layout) {
  const base = path.join(osdRoot, HEADLESS_BASE);
  if (layout === 'directory') return path.join(base, 'index.js');
  if (layout === 'esm-js') return `${base}.js`;
  return `${base}.${layout}`;
}

function makeFixture(
  t,
  {
    target = makeTarget(),
    catalogRuleIds = ['rule-a'],
    coveredRuleIds = catalogRuleIds,
    excludedRules = [],
    cases = defaultCases(coveredRuleIds, excludedRules),
    api = 'valid',
    headlessLayout = 'js',
    includeRecoveredTree = false,
    catalogPresent = true,
  } = {}
) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ppl-osd-grammar-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const osdRoot = path.join(root, 'osd');
  const targetPath = path.join(root, 'resolved-target.json');
  const grammarPath = path.join(root, 'grammar.json');
  const casesPath = path.join(root, 'cases.json');
  const reportPath = path.join(root, 'report.json');
  const summaryPath = path.join(root, 'summary.md');
  fs.mkdirSync(osdRoot, { recursive: true });
  writeJson(targetPath, target);
  writeJson(grammarPath, { grammarHash: HASH });
  writeJson(casesPath, cases);

  if (api !== 'absent') {
    const modulePath = headlessPath(osdRoot, headlessLayout);
    if (api === 'import-failure') {
      writeText(modulePath, `throw new Error('top-level import failed');\n`);
    } else if (api === 'transitive-failure') {
      writeText(modulePath, `require('./missing-transitive-dependency');\n`);
    } else {
      if (headlessLayout === 'esm-js') {
        writeJson(path.join(path.dirname(modulePath), 'package.json'), { type: 'module' });
      }
      writeText(
        modulePath,
        headlessSource({
          format: ['mjs', 'esm-js'].includes(headlessLayout) ? 'esm' : 'cjs',
          missingExport: api === 'missing-export',
          includeRecoveredTree,
        })
      );
    }
  }

  if (catalogPresent) {
    writeText(
      `${path.join(osdRoot, CATALOG_BASE)}.js`,
      `exports.getBundledCatalog = () => ${JSON.stringify(
        catalogRuleIds.map((id) => ({ id }))
      )};\n`
    );
  }

  return {
    root,
    osdRoot,
    targetPath,
    grammarPath,
    casesPath,
    reportPath,
    summaryPath,
  };
}

function invoke(fixture) {
  return spawnSync(
    process.execPath,
    [
      SCRIPT,
      '--grammar',
      fixture.grammarPath,
      '--cases',
      fixture.casesPath,
      '--target',
      fixture.targetPath,
      '--osd-root',
      fixture.osdRoot,
      '--osd-sha',
      'osd-immutable-sha',
      '--report',
      fixture.reportPath,
      '--summary',
      fixture.summaryPath,
    ],
    { encoding: 'utf8' }
  );
}

function readReport(fixture) {
  return JSON.parse(fs.readFileSync(fixture.reportPath, 'utf8'));
}

test('schema-v2 report records deterministic coverage and exact metadata', (t) => {
  const fixture = makeFixture(t, {
    target: makeTarget({
      branch: 'main',
      sqlVersion: '3.9.0',
      osdVersion: '3.8.2',
      osdRef: 'main',
    }),
    catalogRuleIds: ['rule-b', 'rule-a', 'explain-rule'],
    coveredRuleIds: ['rule-b', 'rule-a'],
    excludedRules: [exclusion('explain-rule')],
  });

  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.schemaVersion, 2);
  assert.equal(report.status, 'passed');
  assert.equal(report.sql.version, '3.9.0');
  assert.equal(report.osd.version, '3.8.2');
  assert.equal(report.osd.sha, 'osd-immutable-sha');
  assert.equal(report.grammarHash, HASH);
  assert.deepEqual(report.coverage.catalogRuleIds, ['explain-rule', 'rule-a', 'rule-b']);
  assert.deepEqual(report.coverage.requiredRuleIds, ['rule-a', 'rule-b']);
  assert.deepEqual(report.coverage.coveredRuleIds, ['rule-a', 'rule-b']);
  assert.deepEqual(report.coverage.excludedRuleIds, ['explain-rule']);
  assert.deepEqual(report.coverage.missingRuleIds, []);
  assert.deepEqual(report.coverage.unexpectedRuleIds, []);
  assert.deepEqual(report.coverage.counts, {
    catalog: 3,
    required: 2,
    covered: 2,
    excluded: 1,
    missing: 0,
    unexpected: 0,
  });
  assert.deepEqual(report.rules, {
    catalog: 3,
    required: 2,
    excluded: 1,
    selected: 2,
    passed: 2,
    failed: 0,
  });
});

test('exact release branch version mismatch is structural and writes a report', (t) => {
  const fixture = makeFixture(t, {
    target: makeTarget({ branch: '3.8', sqlVersion: '3.9.0' }),
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /requires SQL and OSD 3\.8\.x versions/);
  assert.equal(readReport(fixture).status, 'error');
});

test('explicit release-line bypass and manual override remain visible', (t) => {
  const target = makeTarget({
    branch: '3.8',
    sqlVersion: '3.9.0',
    osdVersion: '3.7.2',
    osdRef: 'candidate',
    releaseLineValidationBypassed: true,
  });
  target.osd.override = true;
  const fixture = makeFixture(t, { target });

  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.manualOverride, true);
  assert.equal(report.releaseLineValidationBypassed, true);
});

test('true headless target-module absence is the only capability skip', (t) => {
  const fixture = makeFixture(t, { api: 'absent' });
  fs.rmSync(fixture.grammarPath);
  fs.rmSync(fixture.casesPath);

  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.status, 'skipped');
  assert.equal(report.skipReason, 'osd-headless-grammar-api-unavailable');
  assert.equal(report.schemaVersion, 2);
  assert.deepEqual(report.coverage.catalogRuleIds, []);
});

for (const layout of ['js', 'ts', 'mjs', 'esm-js', 'directory']) {
  test(`headless ${layout} module layout is resolved and loaded by the adapter`, (t) => {
    const fixture = makeFixture(t, { headlessLayout: layout });
    const result = invoke(fixture);

    assert.equal(result.status, 0, result.stderr);
    assert.equal(readReport(fixture).status, 'passed');
  });
}

test('resolved target-module import failure is structural, not skipped', (t) => {
  const fixture = makeFixture(t, { api: 'import-failure' });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Could not import OSD module.*top-level import failed/);
  assert.equal(readReport(fixture).status, 'error');
});

test('resolved target-module transitive dependency failure is structural', (t) => {
  const fixture = makeFixture(t, { api: 'transitive-failure' });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /missing-transitive-dependency/);
  assert.equal(readReport(fixture).status, 'error');
});

test('resolved target module with a missing export is structural', (t) => {
  const fixture = makeFixture(t, { api: 'missing-export' });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /must export deserializeBundleOrThrow and lintQueryWithBundle/);
  assert.equal(readReport(fixture).status, 'error');
});

test('missing catalog after a present headless API is structural', (t) => {
  const fixture = makeFixture(t, { catalogPresent: false });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Could not resolve an OSD catalog module/);
  assert.equal(readReport(fixture).status, 'error');
});

test('a malformed bundle fails structurally when capability is present', (t) => {
  const fixture = makeFixture(t);
  fs.writeFileSync(fixture.grammarPath, '{not json');
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Could not .*parse grammar bundle/);
});

test('recovered parse trees are ignored when diagnostics match', (t) => {
  const cases = defaultCases();
  cases.cases[1].query = 'source=accounts | recovered-syntax control';
  const fixture = makeFixture(t, { cases, includeRecoveredTree: true });

  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.status, 'passed');
  assert.ok(report.cases.every((entry) => !('parseTreeCheck' in entry)));
});

test('buildRuntimeTree is not required or reported', (t) => {
  const fixture = makeFixture(t);
  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.ok(report.cases.every((entry) => !('parseTreeCheck' in entry)));
});

test('diagnostic count mismatch returns one and preserves normalized cases', (t) => {
  const cases = defaultCases();
  cases.cases[0].query = 'source=accounts | no-diagnostic';
  const fixture = makeFixture(t, { cases });
  const result = invoke(fixture);

  assert.equal(result.status, 1);
  const report = readReport(fixture);
  assert.equal(report.status, 'failed');
  assert.equal(report.cases.length, 2);
  assert.equal(report.cases[0].actualCount, 0);
  assert.equal(report.cases[1].status, 'passed');
});

test('non-target diagnostics fail even when the target count matches', (t) => {
  const cases = defaultCases();
  cases.cases[0].query = 'source=accounts | target-diagnostic wrong-rule';
  const fixture = makeFixture(t, { cases });
  const result = invoke(fixture);

  assert.equal(result.status, 1);
  const report = readReport(fixture);
  assert.equal(report.cases[0].actualCount, 1);
  assert.deepEqual(report.cases[0].unexpectedDiagnosticRuleIds, ['some-other-rule']);
  assert.match(report.cases[0].error, /non-target rule/);
  assert.deepEqual(report.failures[0].unexpectedDiagnosticRuleIds, ['some-other-rule']);
});

test('one lint exception does not hide later cases', (t) => {
  const cases = defaultCases(['rule-a', 'rule-b']);
  cases.cases[0].query = 'source=accounts | throws';
  const fixture = makeFixture(t, {
    catalogRuleIds: ['rule-a', 'rule-b'],
    cases,
  });
  const result = invoke(fixture);

  assert.equal(result.status, 1);
  assert.match(result.stdout, /PASSED rule-b\/rule-b-trigger/);
  const report = readReport(fixture);
  assert.equal(report.cases.length, 4);
  assert.equal(report.cases[0].error, 'detector crashed');
  assert.equal(report.cases[2].status, 'passed');
});

test('deleting a rule corpus fails catalog coverage with deterministic missing data', (t) => {
  const fixture = makeFixture(t, {
    catalogRuleIds: ['rule-b', 'rule-a'],
    coveredRuleIds: ['rule-a'],
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /missing: rule-b/);
  const report = readReport(fixture);
  assert.deepEqual(report.coverage.requiredRuleIds, ['rule-a', 'rule-b']);
  assert.deepEqual(report.coverage.coveredRuleIds, ['rule-a']);
  assert.deepEqual(report.coverage.missingRuleIds, ['rule-b']);
  assert.equal(report.coverage.counts.missing, 1);
});

test('a newly added catalog rule fails until it is classified', (t) => {
  const fixture = makeFixture(t, {
    catalogRuleIds: ['rule-a', 'new-rule'],
    coveredRuleIds: ['rule-a'],
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /missing: new-rule/);
  assert.deepEqual(readReport(fixture).coverage.missingRuleIds, ['new-rule']);
});

test('a reasoned exclusion removes a catalog rule from the required set', (t) => {
  const fixture = makeFixture(t, {
    catalogRuleIds: ['rule-a', 'explain-rule'],
    coveredRuleIds: ['rule-a'],
    excludedRules: [exclusion('explain-rule', 'Requires backend explain data.')],
  });
  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.deepEqual(report.coverage.requiredRuleIds, ['rule-a']);
  assert.deepEqual(report.coverage.excludedRules, [
    { ruleId: 'explain-rule', reason: 'Requires backend explain data.' },
  ]);
});

test('a stale exclusion is structural and reported as unexpected', (t) => {
  const fixture = makeFixture(t, {
    catalogRuleIds: ['rule-a'],
    coveredRuleIds: ['rule-a'],
    excludedRules: [exclusion('removed-rule')],
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Excluded rule.*absent.*removed-rule/);
  assert.deepEqual(readReport(fixture).coverage.unexpectedRuleIds, ['removed-rule']);
});

test('duplicate exclusion IDs are structural', (t) => {
  const duplicate = [exclusion('explain-rule'), exclusion('explain-rule', 'Second reason.')];
  const fixture = makeFixture(t, {
    catalogRuleIds: ['rule-a', 'explain-rule'],
    coveredRuleIds: ['rule-a'],
    excludedRules: duplicate,
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Duplicate exclusion rule ID.*explain-rule/);
  assert.deepEqual(readReport(fixture).coverage.catalogRuleIds, ['explain-rule', 'rule-a']);
});

test('duplicate case IDs are structural and retain coverage details', (t) => {
  const cases = defaultCases();
  cases.cases[1].id = cases.cases[0].id;
  const fixture = makeFixture(t, { cases });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Duplicate case ID.*rule-a-trigger/);
  assert.deepEqual(readReport(fixture).coverage.coveredRuleIds, ['rule-a']);
});

test('a rule cannot be both covered and excluded', (t) => {
  const fixture = makeFixture(t, {
    excludedRules: [exclusion('rule-a')],
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /both covered and excluded: rule-a/);
});

test('a case naming an unknown OSD rule is structural and reported', (t) => {
  const cases = defaultCases(['rule-a', 'unknown-rule']);
  const fixture = makeFixture(t, {
    catalogRuleIds: ['rule-a'],
    cases,
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Case rule.*absent.*unknown-rule/);
  assert.deepEqual(readReport(fixture).coverage.unexpectedRuleIds, ['unknown-rule']);
});

test('every required rule needs trigger and control classifications', (t) => {
  const cases = defaultCases();
  cases.cases.pop();
  const fixture = makeFixture(t, { cases });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /must have trigger and control cases/);
  assert.equal(readReport(fixture).status, 'error');
});

test('case documents must use schema version two', (t) => {
  const cases = defaultCases();
  cases.schemaVersion = 1;
  const fixture = makeFixture(t, { cases });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /schemaVersion must be 2/);
});

test('the repository corpus covers the 12 default-enabled catalog rules', () => {
  const corpus = JSON.parse(fs.readFileSync(CORPUS, 'utf8'));
  const coveredRuleIds = [...new Set(corpus.cases.map((entry) => entry.ruleId))].sort();
  const excludedRuleIds = corpus.excludedRules.map((entry) => entry.ruleId).sort();

  assert.equal(corpus.schemaVersion, 2);
  assert.equal(corpus.cases.length, 24);
  assert.deepEqual(coveredRuleIds, [
    'agg-on-text',
    'division-by-zero',
    'enabled-false-object',
    'field-validation',
    'invalid-capture-group-name',
    'multisearch-min-subsearch',
    'replace-wildcard-asymmetry',
    'rex-scan-cost',
    'type-mismatch-numeric',
    'union-min-datasets',
    'unsupported-window-function-in-eventstats',
    'wildcard-source-zero-match',
  ]);
  assert.deepEqual(excludedRuleIds, [
    'dedup-consecutive-unsupported',
    'disabled-join-type',
    'flat-object-subfield',
    'head-without-sort',
    'operation-not-pushed',
    'operation-pushed-as-script',
  ]);
  for (const ruleId of coveredRuleIds) {
    const kinds = corpus.cases
      .filter((entry) => entry.ruleId === ruleId)
      .map((entry) => entry.kind)
      .sort();
    assert.deepEqual(kinds, ['control', 'trigger'], ruleId);
  }
});

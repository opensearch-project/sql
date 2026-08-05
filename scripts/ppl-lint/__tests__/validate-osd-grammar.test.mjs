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

const SCRIPT = fileURLToPath(
  new URL('../validate-osd-grammar.mjs', import.meta.url)
);
const HASH = `sha256:${'a'.repeat(64)}`;
const HEADLESS = path.join(
  'src',
  'plugins',
  'data',
  'public',
  'antlr',
  'opensearch_ppl',
  'headless_ppl_lint.js'
);
const CATALOG = path.join(
  'packages',
  'osd-monaco',
  'src',
  'ppl',
  'lint',
  'catalog.js'
);

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(value));
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

function defaultCases(ruleIds = ['rule-a']) {
  return {
    schemaVersion: 1,
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

function headlessSource({
  missingExport = false,
  includeBuildTree = true,
  directTree = false,
} = {}) {
  return `
exports.deserializeBundleOrThrow = (bundle) => ({
  grammarHash: bundle.grammarHash
});
${includeBuildTree ? `
exports.buildRuntimeTree = (query) =>
  query.includes('no-tree')
    ? undefined
    : query.includes('syntax-error')
      ? { tree: { children: [{ constructor: { name: 'ErrorNode' } }] } }
      : ${directTree ? '{}' : '{ tree: {} }'};` : ''}
${missingExport ? '' : `
exports.lintQueryWithBundle = (query, grammar, context) => {
  if (query.includes('throws')) throw new Error('detector crashed');
  const target = Object.entries(context.overrides)
    .find(([, override]) => override.enabled)?.[0];
  if (query.includes('wrong-rule')) {
    return { diagnostics: [{ ruleId: 'some-other-rule' }] };
  }
  return {
    diagnostics: query.includes('trigger') ? [{ ruleId: target }] : []
  };
};`}
`;
}

function makeFixture(
  t,
  {
    target = makeTarget(),
    ruleIds = ['rule-a'],
    cases = defaultCases(ruleIds),
    api = 'valid',
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
    const headlessPath = path.join(osdRoot, HEADLESS);
    fs.mkdirSync(path.dirname(headlessPath), { recursive: true });
    fs.writeFileSync(
      headlessPath,
      headlessSource({
        missingExport: api === 'missing-export',
        includeBuildTree: api !== 'no-build-tree',
        directTree: api === 'direct-tree',
      })
    );
    const catalogPath = path.join(osdRoot, CATALOG);
    fs.mkdirSync(path.dirname(catalogPath), { recursive: true });
    fs.writeFileSync(
      catalogPath,
      `exports.getBundledCatalog = () => ${JSON.stringify(
        ruleIds.map((id) => ({ id }))
      )};`
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

function invoke(fixture, extraArgs = []) {
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
      ...extraArgs,
    ],
    { encoding: 'utf8' }
  );
}

function readReport(fixture) {
  return JSON.parse(fs.readFileSync(fixture.reportPath, 'utf8'));
}

test('main accepts different SQL and OSD product lines and records exact metadata', (t) => {
  const fixture = makeFixture(t, {
    target: makeTarget({
      branch: 'main',
      sqlVersion: '3.9.0',
      osdVersion: '3.8.2',
      osdRef: 'main',
    }),
  });

  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.status, 'passed');
  assert.equal(report.sql.version, '3.9.0');
  assert.equal(report.osd.version, '3.8.2');
  assert.equal(report.osd.sha, 'osd-immutable-sha');
  assert.equal(report.manualOverride, false);
  assert.equal(report.grammarHash, HASH);
  assert.match(fs.readFileSync(fixture.summaryPath, 'utf8'), /Status: \*\*passed\*\*/);
});

test('matching exact release versions accept a RuntimeParseOutcome tree', (t) => {
  const fixture = makeFixture(t);
  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.cases[0].parseTreeCheck, 'verified');
  assert.deepEqual(report.rules, {
    selected: 1,
    passed: 1,
    failed: 0,
  });
});

test('matching exact release versions accept a direct ParserRuleContext', (t) => {
  const fixture = makeFixture(t, { api: 'direct-tree' });
  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.status, 'passed');
  assert.ok(report.cases.every((entry) => entry.parseTreeCheck === 'verified'));
});

test('exact release branch version mismatch is structural and writes a report', (t) => {
  const fixture = makeFixture(t, {
    target: makeTarget({ branch: '3.8', sqlVersion: '3.9.0' }),
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /requires SQL and OSD 3\.8\.x versions/);
  const report = readReport(fixture);
  assert.equal(report.status, 'error');
  assert.equal(report.releaseLineValidationBypassed, false);
  assert.match(report.error, /SQL 3\.9\.0/);
});

test('explicit release-line bypass permits mismatched product lines', (t) => {
  const fixture = makeFixture(t, {
    target: makeTarget({
      branch: '3.8',
      sqlVersion: '3.9.0',
      osdVersion: '3.7.2',
      releaseLineValidationBypassed: true,
    }),
  });
  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.status, 'passed');
  assert.equal(report.releaseLineValidationBypassed, true);
  assert.equal(report.sql.version, '3.9.0');
  assert.equal(report.osd.version, '3.7.2');
  assert.match(
    fs.readFileSync(fixture.summaryPath, 'utf8'),
    /Release-line validation bypassed: `true`/
  );
});

test('manual OSD override is explicit in the report and summary', (t) => {
  const target = makeTarget({ branch: 'main', osdRef: 'candidate' });
  target.osd.override = true;
  const fixture = makeFixture(t, { target });
  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.manualOverride, true);
  assert.match(fs.readFileSync(fixture.summaryPath, 'utf8'), /Manual OSD override: `true`/);
});

test('absent headless API skips before grammar and cases are read', (t) => {
  const fixture = makeFixture(t, { api: 'absent' });
  fs.rmSync(fixture.grammarPath);
  fs.rmSync(fixture.casesPath);

  const result = invoke(fixture);

  assert.equal(result.status, 0, result.stderr);
  const report = readReport(fixture);
  assert.equal(report.status, 'skipped');
  assert.equal(
    report.skipReason,
    'osd-headless-grammar-api-unavailable'
  );
  assert.equal(report.caseCounts.selected, 0);
  assert.deepEqual(report.cases, []);
});

test('an advertised API with a missing export fails structurally', (t) => {
  const fixture = makeFixture(t, { api: 'missing-export' });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /must export deserializeBundleOrThrow and lintQueryWithBundle/);
  assert.equal(readReport(fixture).status, 'error');
});

test('a malformed bundle fails structurally when capability is present', (t) => {
  const fixture = makeFixture(t);
  fs.writeFileSync(fixture.grammarPath, '{not json');
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /Could not .*parse grammar bundle/);
  assert.equal(readReport(fixture).status, 'error');
});

test('diagnostic count mismatch returns one and preserves every normalized case', (t) => {
  const cases = defaultCases();
  cases.cases[0].query = 'source=accounts | wrong-rule trigger';
  const fixture = makeFixture(t, { cases });
  const result = invoke(fixture);

  assert.equal(result.status, 1);
  const report = readReport(fixture);
  assert.equal(report.status, 'failed');
  assert.equal(report.cases.length, 2);
  assert.equal(report.cases[0].status, 'failed');
  assert.equal(report.cases[0].actualCount, 0);
  assert.equal(report.cases[1].status, 'passed');
  assert.deepEqual(report.failures[0], {
    ruleId: 'rule-a',
    caseId: 'rule-a-trigger',
    query: 'source=accounts | wrong-rule trigger',
    expectedCount: 1,
    actualCount: 0,
  });
});

test('one case exception does not hide later cases and the failure report exists', (t) => {
  const cases = defaultCases(['rule-a', 'rule-b']);
  cases.cases[0].query = 'source=accounts | throws';
  const fixture = makeFixture(t, {
    ruleIds: ['rule-a', 'rule-b'],
    cases,
  });
  const result = invoke(fixture);

  assert.equal(result.status, 1);
  assert.match(result.stdout, /PASSED rule-b\/rule-b-trigger/);
  const report = readReport(fixture);
  assert.equal(report.cases.length, 4);
  assert.equal(report.cases[0].error, 'detector crashed');
  assert.equal(report.cases[2].status, 'passed');
  assert.deepEqual(report.rules, {
    selected: 2,
    passed: 1,
    failed: 1,
  });
});

test('a missing parse tree is a case failure and later cases still execute', (t) => {
  const cases = defaultCases();
  cases.cases[0].query = 'source=accounts | no-tree';
  const fixture = makeFixture(t, { cases });
  const result = invoke(fixture);

  assert.equal(result.status, 1);
  const report = readReport(fixture);
  assert.match(report.cases[0].error, /no parse tree/);
  assert.equal(report.cases[1].status, 'passed');
});

test('a recovered syntax error cannot pass as a zero-diagnostic control', (t) => {
  const cases = defaultCases();
  cases.cases[1].query = 'source=accounts | syntax-error';
  const fixture = makeFixture(t, { cases });
  const result = invoke(fixture);

  assert.equal(result.status, 1);
  const report = readReport(fixture);
  assert.equal(report.cases[0].status, 'passed');
  assert.equal(report.cases[1].status, 'failed');
  assert.match(report.cases[1].error, /recovered from a syntax error/);
});

test('controls fail explicitly when buildRuntimeTree is not exported', (t) => {
  const fixture = makeFixture(t, { api: 'no-build-tree' });
  const result = invoke(fixture);

  assert.equal(result.status, 1);
  const report = readReport(fixture);
  assert.equal(report.cases[0].status, 'passed');
  assert.equal(
    report.cases[0].parseTreeCheck,
    'inferred-from-target-diagnostic'
  );
  assert.equal(report.cases[1].status, 'failed');
  assert.equal(report.cases[1].parseTreeCheck, 'unavailable');
  assert.match(report.cases[1].error, /control parse tree cannot be verified/);
});

test('anti-vacuous case validation requires trigger and control coverage per rule', (t) => {
  const fixture = makeFixture(t, {
    cases: {
      schemaVersion: 1,
      cases: [
        {
          id: 'only-trigger',
          ruleId: 'rule-a',
          kind: 'trigger',
          query: 'source=accounts | rule-a trigger',
          expectedCount: 1,
        },
      ],
    },
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /must have trigger and control cases/);
  assert.equal(readReport(fixture).status, 'error');
});

test('a case naming a missing OSD rule fails structurally', (t) => {
  const fixture = makeFixture(t, {
    ruleIds: ['different-rule'],
    cases: defaultCases(['rule-a']),
  });
  const result = invoke(fixture);

  assert.equal(result.status, 2);
  assert.match(result.stderr, /names missing OSD rule "rule-a"/);
  assert.equal(readReport(fixture).status, 'error');
});

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

const ROOT = fileURLToPath(new URL('../../../', import.meta.url));
const WORKFLOW = path.join(
  ROOT,
  '.github',
  'workflows',
  'ppl-lint-grammar-compatibility.yml'
);
const WRAPPER = path.join(ROOT, 'scripts', 'ppl-lint-rule-validation.sh');
const SOURCE = fs.readFileSync(WORKFLOW, 'utf8');
const WRAPPER_SOURCE = fs.readFileSync(WRAPPER, 'utf8');

function stepScript(name) {
  const lines = SOURCE.split('\n');
  const step = lines.indexOf(`      - name: ${name}`);
  assert.notEqual(step, -1, `missing workflow step ${name}`);
  const next = lines.findIndex(
    (line, index) => index > step && line.startsWith('      - name: ')
  );
  const end = next === -1 ? lines.length : next;
  const run = lines.findIndex(
    (line, index) => index > step && index < end && line === '        run: |'
  );
  assert.notEqual(run, -1, `step ${name} has no shell block`);
  return lines
    .slice(run + 1, end)
    .map((line) => line.replace(/^ {10}/, ''))
    .join('\n')
    .trimEnd();
}

function temporaryDirectory(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'ppl-workflow-pairing-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  return directory;
}

function initializeSqlCheckout(t, version) {
  const directory = temporaryDirectory(t);
  fs.writeFileSync(
    path.join(directory, 'build.gradle'),
    `opensearch_version = System.getProperty("opensearch.version", "${version}")\n`
  );
  assert.equal(spawnSync('git', ['init', '--quiet'], { cwd: directory }).status, 0);
  assert.equal(spawnSync('git', ['add', 'build.gradle'], { cwd: directory }).status, 0);
  const commit = spawnSync(
    'git',
    [
      '-c',
      'user.name=Workflow Test',
      '-c',
      'user.email=workflow-test@example.com',
      'commit',
      '--quiet',
      '-m',
      'fixture',
    ],
    { cwd: directory, encoding: 'utf8' }
  );
  assert.equal(commit.status, 0, commit.stderr);
  return directory;
}

function initializeOsdCheckout(t, version = '3.7.0') {
  const directory = temporaryDirectory(t);
  fs.writeFileSync(
    path.join(directory, 'package.json'),
    `${JSON.stringify({ version })}\n`
  );
  assert.equal(spawnSync('git', ['init', '--quiet'], { cwd: directory }).status, 0);
  assert.equal(spawnSync('git', ['add', 'package.json'], { cwd: directory }).status, 0);
  const commit = spawnSync(
    'git',
    [
      '-c',
      'user.name=Workflow Test',
      '-c',
      'user.email=workflow-test@example.com',
      'commit',
      '--quiet',
      '-m',
      'fixture',
    ],
    { cwd: directory, encoding: 'utf8' }
  );
  assert.equal(commit.status, 0, commit.stderr);
  return directory;
}

function gitHead(directory) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], {
    cwd: directory,
    encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim();
}

function readOutputs(file) {
  return Object.fromEntries(
    fs
      .readFileSync(file, 'utf8')
      .trim()
      .split('\n')
      .map((line) => {
        const separator = line.indexOf('=');
        return [line.slice(0, separator), line.slice(separator + 1)];
      })
  );
}

function resolveTarget(
  t,
  {
    version = '4.2.1-SNAPSHOT',
    event = 'pull_request',
    target = 'main',
    osdRef = '',
  } = {}
) {
  const directory = initializeSqlCheckout(t, version);
  const output = path.join(directory, 'github-output');
  const result = spawnSync('/bin/bash', ['-c', stepScript('Resolve SQL target and OSD pair')], {
    cwd: directory,
    encoding: 'utf8',
    env: {
      ...process.env,
      EVENT_NAME: event,
      TARGET_BRANCH: target,
      SQL_HEAD_SHA: 'pull-request-head',
      REQUESTED_OSD_REF: osdRef,
      GITHUB_OUTPUT: output,
    },
  });
  return {
    result,
    outputs: fs.existsSync(output) ? readOutputs(output) : {},
  };
}

test('workflow has focused triggers, read-only permissions, and no fixed product version', () => {
  assert.match(SOURCE, /^  pull_request:$/m);
  assert.match(SOURCE, /^  push:$/m);
  assert.match(SOURCE, /^  workflow_dispatch:$/m);
  assert.equal(SOURCE.match(/^      - '\[0-9\]\+\.\[0-9\]\+'$/gm)?.length, 2);
  assert.equal(SOURCE.match(/^      - main$/gm)?.length, 2);
  assert.match(SOURCE, /^permissions:\n  contents: read$/m);
  assert.doesNotMatch(SOURCE, /^\s+schedule:$/m);
  assert.doesNotMatch(SOURCE, /\b(?:secrets|vars)\./);
  assert.doesNotMatch(SOURCE, /compiled-version|latestEligibleGa|release-tags/);
  assert.doesNotMatch(SOURCE, /allow_release_line_mismatch|inputs\.osd_repo|REQUESTED_OSD_REPO/);
  assert.doesNotMatch(SOURCE, /settings\.gradle/);
  assert.doesNotMatch(SOURCE, /plugin\/(?:build\.gradle|src\/main)/);
  assert.equal(
    SOURCE.match(
      /ppl\/src\/main\/java\/org\/opensearch\/sql\/ppl\/autocomplete\/PPLGrammarBundleExporter\.java/g
    )
      ?.length,
    2
  );

  const fixedProductVersions = [
    ...SOURCE.matchAll(/(?:^|[^0-9.])([0-9]+\.[0-9]+\.[0-9]+)(?![0-9.])/gm),
  ].map((match) => match[1]);
  assert.deepEqual(fixedProductVersions, []);
});

test('workflow and wrapper delegate capability to one adapter invocation', () => {
  const exportGrammar = SOURCE.indexOf('      - name: Export candidate runtime grammar');
  const java = SOURCE.indexOf('      - name: Set up JDK 21');
  const bootstrap = SOURCE.indexOf('      - name: Bootstrap OpenSearch Dashboards');
  const validate = SOURCE.indexOf('      - name: Validate OSD linter against candidate grammar');
  assert.ok(java < exportGrammar);
  assert.ok(exportGrammar < bootstrap);
  assert.ok(bootstrap < validate);

  const exportScript = stepScript('Export candidate runtime grammar');
  assert.match(exportScript, /\.\/gradlew :ppl:exportPplGrammarBundle --no-daemon/);
  assert.match(
    exportScript,
    /"-PpplGrammarBundleOutput=\$GITHUB_WORKSPACE\/ppl-grammar-bundle\.json"/
  );
  const validateScript = stepScript('Validate OSD linter against candidate grammar');
  assert.match(validateScript, /^set \+e$/m);
  assert.match(validateScript, /^node -r \.\/src\/setup_node_env/m);
  assert.match(validateScript, /--summary "\$GITHUB_STEP_SUMMARY"/);

  assert.equal(
    SOURCE.match(/scripts\/ppl-lint\/validate-osd-grammar\.mjs/g)?.length,
    1
  );
  assert.equal(WRAPPER_SOURCE.match(/node -r \.\/src\/setup_node_env/g)?.length, 1);
  for (const content of [SOURCE, WRAPPER_SOURCE]) {
    assert.doesNotMatch(content, /Detect headless grammar capability/);
    assert.doesNotMatch(content, /headless_ppl_lint/);
    assert.doesNotMatch(content, /steps\.capability/);
    assert.doesNotMatch(content, /opensearch-sql-plugin:run/);
    assert.doesNotMatch(content, /\bcurl\b|127\.0\.0\.1:9200|ppl-grammar-cluster\.log/);
    assert.doesNotMatch(content, /\btrap\b|gradle_pid|GRADLE_PID/);
  }
  assert.match(WRAPPER_SOURCE, /\.\/gradlew :ppl:exportPplGrammarBundle --no-daemon/);
});

test('pre-adapter failure fallback writes the structural report contract without target metadata', (t) => {
  const fallback = stepScript('Record pre-report failure');
  assert.match(fallback, /ppl-lint-grammar-compatibility-report\.json/);
  assert.match(fallback, /schemaVersion: 2/);
  assert.match(fallback, /status: "error"/);
  assert.match(fallback, /error: \$error/);
  assert.match(fallback, /manualOverride: \(\.osd\.override \/\/ false\)/);
  assert.match(fallback, /releaseLineValidationBypassed: false/);
  assert.match(fallback, /catalogRuleIds: \[\]/);
  assert.match(fallback, /excludedRuleIds: \[\]/);
  assert.match(fallback, /excludedRules: \[\]/);
  assert.match(fallback, /caseCounts: \{selected: 0, passed: 0, failed: 0\}/);
  assert.match(fallback, /cases: \[\]/);
  assert.match(fallback, /failures: \[\]/);

  const fallbackIndex = SOURCE.indexOf('      - name: Record pre-report failure');
  const uploadIndex = SOURCE.indexOf('      - name: Upload grammar compatibility artifacts');
  const enforceIndex = SOURCE.indexOf('      - name: Enforce compatibility result');
  assert.ok(fallbackIndex < uploadIndex);
  assert.ok(uploadIndex < enforceIndex);

  const directory = temporaryDirectory(t);
  const summary = path.join(directory, 'summary.md');
  const result = spawnSync('/bin/bash', ['-c', fallback], {
    cwd: directory,
    encoding: 'utf8',
    env: { ...process.env, GITHUB_STEP_SUMMARY: summary },
  });
  assert.equal(result.status, 0, result.stderr);
  const report = JSON.parse(
    fs.readFileSync(
      path.join(directory, 'ppl-lint-grammar-compatibility-report.json'),
      'utf8'
    )
  );
  assert.equal(report.status, 'error');
  assert.equal(report.schemaVersion, 2);
  assert.match(report.error, /before the compatibility adapter/);
  assert.deepEqual(report.rules, {
    catalog: 0,
    required: 0,
    excluded: 0,
    selected: 0,
    passed: 0,
    failed: 0,
  });
  assert.deepEqual(report.coverage.counts, {
    catalog: 0,
    required: 0,
    covered: 0,
    excluded: 0,
    missing: 0,
    unexpected: 0,
  });
  assert.deepEqual(report.coverage.missingRuleIds, []);
  assert.equal('sql' in report, false);
});

test('extracted resolver pairs main and exact release targets', (t) => {
  const main = resolveTarget(t);
  assert.equal(main.result.status, 0, main.result.stderr);
  assert.equal(main.outputs.target_branch, 'main');
  assert.equal(main.outputs.sql_version, '4.2.1');
  assert.equal(main.outputs.osd_repo, 'opensearch-project/OpenSearch-Dashboards');
  assert.equal(main.outputs.osd_ref, 'main');

  const release = resolveTarget(t, { target: '4.2' });
  assert.equal(release.result.status, 0, release.result.stderr);
  assert.equal(release.outputs.sql_release_line, '4.2');
  assert.equal(release.outputs.osd_ref, '4.2');
});

test('extracted resolver rejects mismatches and limits overrides to dispatch', (t) => {
  const mismatch = resolveTarget(t, { target: '4.1' });
  assert.notEqual(mismatch.result.status, 0);
  assert.match(mismatch.result.stdout, /does not match target branch 4\.1/);

  const pushOverride = resolveTarget(t, {
    event: 'push',
    osdRef: 'candidate',
  });
  assert.notEqual(pushOverride.result.status, 0);
  assert.match(pushOverride.result.stdout, /ref overrides are allowed only for workflow_dispatch/);

  const dispatch = resolveTarget(t, {
    event: 'workflow_dispatch',
    osdRef: 'candidate',
  });
  assert.equal(dispatch.result.status, 0, dispatch.result.stderr);
  assert.equal(dispatch.outputs.osd_repo, 'opensearch-project/OpenSearch-Dashboards');
  assert.equal(dispatch.outputs.osd_ref, 'candidate');
  assert.equal(dispatch.outputs.osd_override, 'true');
  assert.equal('release_line_bypass' in dispatch.outputs, false);

  const dispatchMismatch = resolveTarget(t, {
    event: 'workflow_dispatch',
    target: '4.1',
    osdRef: 'candidate',
  });
  assert.notEqual(dispatchMismatch.result.status, 0);
  assert.match(dispatchMismatch.result.stdout, /does not match target branch 4\.1/);

  const featureBranch = resolveTarget(t, { target: 'feature/test' });
  assert.notEqual(featureBranch.result.status, 0);
  assert.match(featureBranch.result.stdout, /must be main or an exact X\.Y release branch/);

  const malformedVersion = resolveTarget(t, { version: '4.2', target: 'main' });
  assert.notEqual(malformedVersion.result.status, 0);
  assert.match(malformedVersion.result.stdout, /Invalid SQL product version/);
});

test('wrapper rejects stale target metadata before exporting a grammar', (t) => {
  const directory = temporaryDirectory(t);
  const osdRoot = initializeOsdCheckout(t);
  const target = path.join(directory, 'target.json');
  const report = path.join(directory, 'report.json');
  const summary = path.join(directory, 'summary.md');
  const sqlVersionRaw = fs
    .readFileSync(path.join(ROOT, 'build.gradle'), 'utf8')
    .match(/opensearch_version = System\.getProperty\("opensearch\.version", "([^"]+)"\)/)[1];
  const sqlVersion = sqlVersionRaw.replace(/[-+].*$/, '');
  fs.writeFileSync(
    target,
    JSON.stringify({
      sql: {
        sha: 'stale-sql-sha',
        targetBranch: 'main',
        versionRaw: sqlVersionRaw,
        version: sqlVersion,
      },
      osd: {
        repository: 'local/OpenSearch-Dashboards',
        ref: 'main',
        sha: gitHead(osdRoot),
        version: '3.7.0',
      },
    })
  );

  const result = spawnSync(
    WRAPPER,
    ['--target', target, '--osd-root', osdRoot, '--report', report, '--summary', summary],
    { cwd: ROOT, encoding: 'utf8' }
  );
  assert.equal(result.status, 2);
  assert.match(result.stderr, /target SQL SHA stale-sql-sha does not match checkout/);
  assert.equal(fs.existsSync(report), false);
});

test('wrapper refuses to label an unrelated local OSD checkout as the paired ref', (t) => {
  const directory = temporaryDirectory(t);
  const osdRoot = initializeOsdCheckout(t);
  assert.equal(
    spawnSync('git', ['checkout', '--quiet', '-b', 'feature'], { cwd: osdRoot }).status,
    0
  );
  const diverge = spawnSync(
    'git',
    [
      '-c',
      'user.name=Workflow Test',
      '-c',
      'user.email=workflow-test@example.com',
      'commit',
      '--quiet',
      '--allow-empty',
      '-m',
      'diverge',
    ],
    { cwd: osdRoot, encoding: 'utf8' }
  );
  assert.equal(diverge.status, 0, diverge.stderr);
  const result = spawnSync(
    WRAPPER,
    [
      '--osd-root',
      osdRoot,
      '--target-branch',
      'main',
      '--report',
      path.join(directory, 'report.json'),
      '--summary',
      path.join(directory, 'summary.md'),
    ],
    { cwd: ROOT, encoding: 'utf8' }
  );

  assert.equal(result.status, 2);
  assert.match(result.stderr, /OSD ref main resolves to .* not checkout/);
});

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
    osdRepo = '',
    osdRef = '',
    bypass = '',
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
      REQUESTED_OSD_REPO: osdRepo,
      REQUESTED_OSD_REF: osdRef,
      REQUESTED_BYPASS: bypass,
      GITHUB_OUTPUT: output,
    },
  });
  return {
    result,
    outputs: fs.existsSync(output) ? readOutputs(output) : {},
  };
}

function probeCapability(t, extension) {
  const directory = temporaryDirectory(t);
  const output = path.join(directory, 'github-output');
  if (extension) {
    const module = path.join(
      directory,
      '.ci',
      'OpenSearch-Dashboards',
      'src',
      'plugins',
      'data',
      'public',
      'antlr',
      'opensearch_ppl',
      `headless_ppl_lint.${extension}`
    );
    fs.mkdirSync(path.dirname(module), { recursive: true });
    fs.writeFileSync(module, '');
  }
  const result = spawnSync('/bin/bash', ['-c', stepScript('Detect headless grammar capability')], {
    cwd: directory,
    encoding: 'utf8',
    env: { ...process.env, GITHUB_OUTPUT: output },
  });
  assert.equal(result.status, 0, result.stderr);
  return readOutputs(output).available;
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
  assert.equal(
    SOURCE.match(/plugin\/src\/main\/java\/org\/opensearch\/sql\/plugin\/SQLPlugin\.java/g)
      ?.length,
    2
  );

  const fixedProductVersions = [
    ...SOURCE.matchAll(/(?:^|[^0-9.])([0-9]+\.[0-9]+\.[0-9]+)(?![0-9.])/gm),
  ].map((match) => match[1]);
  assert.deepEqual(fixedProductVersions, []);
});

test('skip path uses the adapter before bootstrap and supports TypeScript or JavaScript modules', (t) => {
  const capability = SOURCE.indexOf('      - name: Detect headless grammar capability');
  const skip = SOURCE.indexOf('      - name: Record unsupported paired branch with adapter');
  const java = SOURCE.indexOf('      - name: Set up JDK 21');
  const capture = SOURCE.indexOf('      - name: Capture candidate runtime grammar');
  const bootstrap = SOURCE.indexOf('      - name: Bootstrap OpenSearch Dashboards');
  assert.ok(capability < skip);
  assert.ok(skip < java);
  assert.ok(skip < capture);
  assert.ok(skip < bootstrap);

  const skipScript = stepScript('Record unsupported paired branch with adapter');
  assert.match(
    skipScript,
    /^node "\$GITHUB_WORKSPACE\/scripts\/ppl-lint\/validate-osd-grammar\.mjs"/m
  );
  assert.doesNotMatch(skipScript, /node -r /);
  assert.match(skipScript, /--grammar "\$GITHUB_WORKSPACE\/ppl-grammar-bundle\.json"/);
  assert.match(skipScript, /--summary "\$GITHUB_STEP_SUMMARY"/);

  assert.equal(probeCapability(t, 'ts'), 'true');
  assert.equal(probeCapability(t, 'js'), 'true');
  assert.equal(probeCapability(t), 'false');
});

test('pre-adapter failure fallback writes the structural report contract without target metadata', (t) => {
  const fallback = stepScript('Record pre-report failure');
  assert.match(fallback, /ppl-lint-grammar-compatibility-report\.json/);
  assert.match(fallback, /status: "error"/);
  assert.match(fallback, /error: \$error/);
  assert.match(fallback, /manualOverride: \(\.osd\.override \/\/ false\)/);
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
  assert.match(report.error, /before the compatibility adapter/);
  assert.deepEqual(report.rules, { selected: 0, passed: 0, failed: 0 });
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
    osdRepo: 'example/OpenSearch-Dashboards',
  });
  assert.notEqual(pushOverride.result.status, 0);
  assert.match(pushOverride.result.stdout, /allowed only for workflow_dispatch/);

  const dispatch = resolveTarget(t, {
    event: 'workflow_dispatch',
    target: '4.1',
    osdRepo: 'example/OpenSearch-Dashboards',
    osdRef: 'candidate',
    bypass: 'true',
  });
  assert.equal(dispatch.result.status, 0, dispatch.result.stderr);
  assert.equal(dispatch.outputs.osd_repo, 'example/OpenSearch-Dashboards');
  assert.equal(dispatch.outputs.osd_ref, 'candidate');
  assert.equal(dispatch.outputs.osd_override, 'true');
  assert.equal(dispatch.outputs.release_line_bypass, 'true');

  const featureBranch = resolveTarget(t, { target: 'feature/test' });
  assert.notEqual(featureBranch.result.status, 0);
  assert.match(featureBranch.result.stdout, /must be main or an exact X\.Y release branch/);

  const malformedVersion = resolveTarget(t, { version: '4.2', target: 'main' });
  assert.notEqual(malformedVersion.result.status, 0);
  assert.match(malformedVersion.result.stdout, /Invalid SQL product version/);
});

test('wrapper reproduces a dispatch release-line bypass on exact checkouts', (t) => {
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
        sha: gitHead(ROOT),
        targetBranch: '9.9',
        versionRaw: sqlVersionRaw,
        version: sqlVersion,
      },
      osd: {
        repository: 'local/OpenSearch-Dashboards',
        ref: '9.9',
        sha: gitHead(osdRoot),
        version: '3.7.0',
      },
      releaseLineValidationBypassed: true,
    })
  );

  const result = spawnSync(
    WRAPPER,
    ['--target', target, '--osd-root', osdRoot, '--report', report, '--summary', summary],
    { cwd: ROOT, encoding: 'utf8' }
  );
  assert.equal(result.status, 0, result.stderr);
  const output = JSON.parse(fs.readFileSync(report, 'utf8'));
  assert.equal(output.status, 'skipped');
  assert.equal(output.releaseLineValidationBypassed, true);
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

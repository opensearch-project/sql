/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const HEADLESS = 'src/plugins/data/public/antlr/opensearch_ppl/headless_ppl_lint';
const CATALOGS = [
  'packages/osd-monaco/ppl-lint',
  'packages/osd-monaco/src/ppl/lint/catalog',
];
const SKIP_REASON = 'osd-headless-grammar-api-unavailable';
const OPTIONS = ['grammar', 'cases', 'target', 'osd-root', 'osd-sha', 'report', 'summary'];

class StructuralError extends Error {}

function fail(message) {
  throw new StructuralError(message);
}

function parseArgs(argv) {
  if (argv.length === 1 && argv[0] === '--help') {
    return { help: true };
  }
  const args = {};
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i];
    const value = argv[i + 1];
    const key = flag?.startsWith('--') ? flag.slice(2) : '';
    if (!OPTIONS.includes(key) || !value || value.startsWith('--')) {
      fail(`Invalid CLI argument near ${JSON.stringify(flag)}.`);
    }
    if (args[key]) fail(`Duplicate CLI argument ${flag}.`);
    args[key] = value;
  }
  const missing = OPTIONS.filter((key) => !args[key]);
  if (missing.length) fail(`Missing CLI argument(s): ${missing.map((key) => `--${key}`).join(', ')}.`);
  return args;
}

function readJson(file, label) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (error) {
    fail(`Could not read or parse ${label} ${file}: ${error.message}`);
  }
}

function object(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail(`${label} must be an object.`);
  }
  return value;
}

function string(value, label) {
  if (typeof value !== 'string' || !value.trim()) fail(`${label} must be a non-empty string.`);
  return value;
}

function version(value, label) {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:[-+][0-9A-Za-z][0-9A-Za-z.-]*)?$/.exec(
    string(value, label)
  );
  if (!match) fail(`${label} must be a complete product version.`);
  return `${Number(match[1])}.${Number(match[2])}.${Number(match[3])}`;
}

function loadTarget(file, osdSha) {
  const raw = object(readJson(file, 'target'), 'target');
  const sql = object(raw.sql, 'target.sql');
  const osd = object(raw.osd, 'target.osd');
  if (
    raw.releaseLineValidationBypassed !== undefined &&
    typeof raw.releaseLineValidationBypassed !== 'boolean'
  ) {
    fail('target.releaseLineValidationBypassed must be a boolean.');
  }
  const sqlVersion = version(sql.version, 'target.sql.version');
  if (sql.versionRaw && version(sql.versionRaw, 'target.sql.versionRaw') !== sqlVersion) {
    fail(`target.sql.versionRaw does not normalize to ${sqlVersion}.`);
  }
  if (osd.sha && osd.sha !== osdSha) fail('target.osd.sha does not match --osd-sha.');

  return {
    sql: {
      sha: string(sql.sha, 'target.sql.sha'),
      ...(sql.headSha ? { headSha: string(sql.headSha, 'target.sql.headSha') } : {}),
      targetBranch: string(sql.targetBranch, 'target.sql.targetBranch'),
      ...(sql.versionRaw ? { versionRaw: sql.versionRaw } : {}),
      version: sqlVersion,
    },
    osd: {
      repository: string(osd.repository, 'target.osd.repository'),
      ref: string(osd.ref, 'target.osd.ref'),
      sha: string(osdSha, '--osd-sha'),
      version: version(osd.version, 'target.osd.version'),
    },
    manualOverride: raw.manualOverride === true || osd.override === true,
    releaseLineValidationBypassed: raw.releaseLineValidationBypassed === true,
  };
}

function validatePairing(target) {
  const branch = target.sql.targetBranch;
  const release = /^\d+\.\d+$/.test(branch);
  if (branch !== 'main' && !release) fail(`Unsupported SQL target branch ${JSON.stringify(branch)}.`);
  if (!target.manualOverride && target.osd.ref !== branch) {
    fail(`OSD ref ${JSON.stringify(target.osd.ref)} must match SQL target branch ${JSON.stringify(branch)}.`);
  }
  if (release && !target.releaseLineValidationBypassed) {
    const sqlLine = target.sql.version.split('.').slice(0, 2).join('.');
    const osdLine = target.osd.version.split('.').slice(0, 2).join('.');
    if (sqlLine !== branch || osdLine !== branch) {
      fail(
        `Exact release target ${branch} requires SQL and OSD ${branch}.x versions; ` +
          `got SQL ${target.sql.version} and OSD ${target.osd.version}.`
      );
    }
  }
}

function modulePath(root, name) {
  const base = path.join(root, name);
  return [base, `${base}.js`, `${base}.ts`].find((candidate) => fs.existsSync(candidate));
}

function exportsOf(module) {
  return module?.default && typeof module.default === 'object'
    ? { ...module.default, ...module }
    : module;
}

function loadApi(osdRoot) {
  const requireFromOsd = createRequire(path.join(osdRoot, 'noop.js'));
  let headless;
  try {
    headless = exportsOf(requireFromOsd(path.join(osdRoot, HEADLESS)));
  } catch (error) {
    fail(`OSD advertises ${HEADLESS}, but import failed: ${error.message}`);
  }
  if (
    typeof headless?.deserializeBundleOrThrow !== 'function' ||
    typeof headless?.lintQueryWithBundle !== 'function'
  ) {
    fail(`${HEADLESS} must export deserializeBundleOrThrow and lintQueryWithBundle.`);
  }

  let catalogModule;
  const errors = [];
  for (const name of CATALOGS) {
    if (!modulePath(osdRoot, name)) continue;
    try {
      const candidate = exportsOf(requireFromOsd(path.join(osdRoot, name)));
      if (typeof candidate?.getBundledCatalog === 'function') {
        catalogModule = candidate;
        break;
      }
      errors.push(`${name} has no getBundledCatalog export`);
    } catch (error) {
      errors.push(`${name}: ${error.message}`);
    }
  }
  if (!catalogModule) fail(`Could not load OSD catalog${errors.length ? `: ${errors.join('; ')}` : '.'}`);

  let catalog;
  try {
    catalog = catalogModule.getBundledCatalog();
  } catch (error) {
    fail(`getBundledCatalog failed: ${error.message}`);
  }
  if (!Array.isArray(catalog) || !catalog.length) fail('OSD catalog must be a non-empty array.');
  const catalogIds = catalog.map((entry, index) =>
    string(object(entry, `catalog[${index}]`).id, `catalog[${index}].id`)
  );
  if (new Set(catalogIds).size !== catalogIds.length) fail('OSD catalog has duplicate rule IDs.');

  return {
    deserialize: headless.deserializeBundleOrThrow,
    lint: headless.lintQueryWithBundle,
    buildTree: typeof headless.buildRuntimeTree === 'function' ? headless.buildRuntimeTree : undefined,
    catalogIds,
  };
}

function loadGrammar(file, deserialize) {
  const bundle = object(readJson(file, 'grammar bundle'), 'grammar bundle');
  if (typeof bundle.grammarHash !== 'string' || !/^sha256:[0-9a-f]{64}$/i.test(bundle.grammarHash)) {
    fail('grammarHash must be "sha256:" followed by 64 hexadecimal characters.');
  }
  let grammar;
  try {
    grammar = deserialize(bundle);
  } catch (error) {
    fail(`Could not deserialize candidate grammar bundle: ${error.message}`);
  }
  if (!grammar) fail('deserializeBundleOrThrow returned no grammar.');
  if (grammar.grammarHash && grammar.grammarHash !== bundle.grammarHash) {
    fail('Deserialized grammar hash does not match the bundle.');
  }
  return { bundle, grammar };
}

function loadCases(file, catalogIds) {
  const document = readJson(file, 'grammar cases');
  const rawCases = Array.isArray(document) ? document : object(document, 'case document').cases;
  if (!Array.isArray(rawCases) || !rawCases.length) fail('At least one grammar case is required.');
  const knownRules = new Set(catalogIds);
  const ids = new Set();
  const cases = rawCases.map((rawCase, index) => {
    const candidate = object(rawCase, `case[${index}]`);
    const id = string(candidate.id, `case[${index}].id`);
    const ruleId = string(candidate.ruleId, `case ${id}.ruleId`);
    if (ids.has(id)) fail(`Duplicate case ID ${JSON.stringify(id)}.`);
    if (!knownRules.has(ruleId)) fail(`Case ${JSON.stringify(id)} names missing OSD rule ${JSON.stringify(ruleId)}.`);
    ids.add(id);
    if (!['trigger', 'control'].includes(candidate.kind)) fail(`Case ${id} has invalid kind.`);
    if (!Number.isInteger(candidate.expectedCount) || candidate.expectedCount < 0) {
      fail(`Case ${id} expectedCount must be a non-negative integer.`);
    }
    if (candidate.kind === 'trigger' && candidate.expectedCount === 0) {
      fail(`Trigger case ${id} must expect a diagnostic.`);
    }
    if (candidate.kind === 'control' && candidate.expectedCount !== 0) {
      fail(`Control case ${id} must expect zero diagnostics.`);
    }
    const context = candidate.context === undefined ? {} : object(candidate.context, `case ${id}.context`);
    if (['overrides', 'dataSourceVersion', 'knownVersion'].some((key) => key in context)) {
      fail(`Case ${id} context sets an adapter-owned field.`);
    }
    return {
      id,
      ruleId,
      kind: candidate.kind,
      query: string(candidate.query, `case ${id}.query`),
      expectedCount: candidate.expectedCount,
      context,
    };
  });

  for (const ruleId of new Set(cases.map((entry) => entry.ruleId))) {
    const kinds = new Set(cases.filter((entry) => entry.ruleId === ruleId).map((entry) => entry.kind));
    if (!kinds.has('trigger') || !kinds.has('control')) {
      fail(`Selected rule ${JSON.stringify(ruleId)} must have trigger and control cases.`);
    }
  }
  return cases;
}

function contextFor(grammarCase, target, catalogIds) {
  const context = { ...grammarCase.context };
  if (Array.isArray(context.fields)) context.fields = new Set(context.fields);
  if (context.typeMap && !Array.isArray(context.typeMap)) {
    context.typeMap = new Map(Object.entries(context.typeMap));
  }
  if (Array.isArray(context.disabledObjectFields)) {
    context.disabledObjectFields = new Set(context.disabledObjectFields);
  }
  return {
    ...context,
    isCalcite: context.isCalcite !== false,
    dataSourceVersion: target.sql.version,
    knownVersion: target.sql.version,
    overrides: Object.fromEntries(catalogIds.map((id) => [id, { enabled: id === grammarCase.ruleId }])),
  };
}

function hasParseTree(result) {
  if (!result) return false;
  return typeof result === 'object' && 'tree' in result ? Boolean(result.tree) : true;
}

function parseTreeOf(result) {
  return typeof result === 'object' && result && 'tree' in result ? result.tree : result;
}

function hasErrorNode(tree) {
  const pending = [tree];
  while (pending.length) {
    const node = pending.pop();
    if (!node || typeof node !== 'object') continue;
    if (node.constructor?.name === 'ErrorNode') return true;
    if (Array.isArray(node.children)) pending.push(...node.children);
  }
  return false;
}

function executeCases(api, grammar, cases, target) {
  return cases.map((grammarCase) => {
    const result = {
      caseId: grammarCase.id,
      ruleId: grammarCase.ruleId,
      kind: grammarCase.kind,
      query: grammarCase.query,
      expectedCount: grammarCase.expectedCount,
    };
    try {
      if (api.buildTree) {
        const parse = api.buildTree(grammarCase.query, grammar);
        if (!hasParseTree(parse)) throw new Error('candidate parser produced no parse tree');
        if (hasErrorNode(parseTreeOf(parse))) {
          throw new Error('candidate parser recovered from a syntax error');
        }
      }
      const lint = api.lint(grammarCase.query, grammar, contextFor(grammarCase, target, api.catalogIds));
      if (!Array.isArray(lint?.diagnostics)) throw new Error('lintQueryWithBundle returned no diagnostics array');
      result.actualCount = lint.diagnostics.filter((entry) => entry?.ruleId === grammarCase.ruleId).length;
      result.parseTreeCheck = api.buildTree
        ? 'verified'
        : grammarCase.kind === 'trigger' && result.actualCount > 0
          ? 'inferred-from-target-diagnostic'
          : 'unavailable';
      const countMatches = result.actualCount === result.expectedCount;
      const treeVerified = result.parseTreeCheck !== 'unavailable';
      result.status = countMatches && treeVerified ? 'passed' : 'failed';
      if (!treeVerified) {
        result.error = 'buildRuntimeTree is not exported; control parse tree cannot be verified';
      }
    } catch (error) {
      result.actualCount = null;
      result.parseTreeCheck = api.buildTree ? 'failed' : 'unavailable';
      result.status = 'failed';
      result.error = error instanceof Error ? error.message : String(error);
    }
    console[result.status === 'passed' ? 'log' : 'error'](
      `[ppl-lint-grammar] ${result.status.toUpperCase()} ${result.ruleId}/${result.caseId}: ` +
        `expected ${result.expectedCount}, got ${result.actualCount ?? 'error'}`
    );
    return result;
  });
}

function counts(results) {
  const passed = results.filter((entry) => entry.status === 'passed').length;
  return { selected: results.length, passed, failed: results.length - passed };
}

function makeReport(target, grammarHash, cases) {
  const ruleResults = [...new Set(cases.map((entry) => entry.ruleId))].map((ruleId) => ({
    status: cases.filter((entry) => entry.ruleId === ruleId).every((entry) => entry.status === 'passed')
      ? 'passed'
      : 'failed',
  }));
  const failures = cases.filter((entry) => entry.status === 'failed').map((entry) => ({
    ruleId: entry.ruleId,
    caseId: entry.caseId,
    query: entry.query,
    expectedCount: entry.expectedCount,
    actualCount: entry.actualCount,
    ...(entry.error ? { error: entry.error } : {}),
  }));
  return {
    schemaVersion: 1,
    status: failures.length ? 'failed' : 'passed',
    sql: target.sql,
    osd: target.osd,
    manualOverride: target.manualOverride,
    releaseLineValidationBypassed: target.releaseLineValidationBypassed,
    grammarHash,
    rules: counts(ruleResults),
    caseCounts: counts(cases),
    cases,
    failures,
  };
}

function emptyReport(status, target, extra = {}) {
  return {
    schemaVersion: 1,
    status,
    ...extra,
    ...(target
      ? {
          sql: target.sql,
          osd: target.osd,
          manualOverride: target.manualOverride,
          releaseLineValidationBypassed: target.releaseLineValidationBypassed,
        }
      : {}),
    rules: { selected: 0, passed: 0, failed: 0 },
    caseCounts: { selected: 0, passed: 0, failed: 0 },
    cases: [],
    failures: [],
  };
}

function summary(report) {
  const lines = [`## PPL grammar compatibility`, '', `- Status: **${report.status}**`];
  if (report.skipReason) lines.push(`- Skip reason: \`${report.skipReason}\``);
  if (report.sql) lines.push(`- SQL: \`${report.sql.targetBranch}\` / \`${report.sql.version}\` / \`${report.sql.sha}\``);
  if (report.osd) lines.push(`- OSD: \`${report.osd.ref}\` / \`${report.osd.version}\` / \`${report.osd.sha}\``);
  if (typeof report.manualOverride === 'boolean') {
    lines.push(`- Manual OSD override: \`${report.manualOverride}\``);
  }
  if (typeof report.releaseLineValidationBypassed === 'boolean') {
    lines.push(`- Release-line validation bypassed: \`${report.releaseLineValidationBypassed}\``);
  }
  if (report.grammarHash) lines.push(`- Grammar: \`${report.grammarHash}\``);
  lines.push(`- Rules: ${report.rules.selected} selected, ${report.rules.passed} passed, ${report.rules.failed} failed`);
  if (report.error) lines.push('', `Structural error: ${report.error.replaceAll('\n', ' ')}`);
  if (report.failures.length) {
    lines.push('', '| Rule | Case | Expected | Actual | Error |', '| --- | --- | ---: | ---: | --- |');
    for (const failure of report.failures) {
      lines.push(
        `| ${failure.ruleId} | ${failure.caseId} | ${failure.expectedCount} | ` +
          `${failure.actualCount ?? 'n/a'} | ${(failure.error || '').replaceAll('|', '\\|')} |`
      );
    }
  }
  return `${lines.join('\n')}\n`;
}

function writeOutputs(args, report) {
  fs.mkdirSync(path.dirname(path.resolve(args.report)), { recursive: true });
  fs.writeFileSync(args.report, `${JSON.stringify(report, null, 2)}\n`);
  fs.mkdirSync(path.dirname(path.resolve(args.summary)), { recursive: true });
  fs.appendFileSync(args.summary, summary(report));
}

export function run(argv = process.argv.slice(2)) {
  let args;
  let target;
  try {
    args = parseArgs(argv);
    if (args.help) {
      console.log('See --grammar, --cases, --target, --osd-root, --osd-sha, --report, and --summary.');
      return 0;
    }
    target = loadTarget(args.target, args['osd-sha']);
    validatePairing(target);
    const osdRoot = path.resolve(args['osd-root']);
    if (!fs.existsSync(osdRoot) || !fs.statSync(osdRoot).isDirectory()) fail(`Invalid OSD root ${osdRoot}.`);
    if (!modulePath(osdRoot, HEADLESS)) {
      writeOutputs(args, emptyReport('skipped', target, { skipReason: SKIP_REASON }));
      return 0;
    }
    const api = loadApi(osdRoot);
    const { bundle, grammar } = loadGrammar(args.grammar, api.deserialize);
    const cases = loadCases(args.cases, api.catalogIds);
    const report = makeReport(target, bundle.grammarHash, executeCases(api, grammar, cases, target));
    writeOutputs(args, report);
    return report.status === 'passed' ? 0 : 1;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[ppl-lint-grammar] ERROR: ${message}`);
    if (args?.report && args?.summary) {
      try {
        writeOutputs(args, emptyReport('error', target, { error: message }));
      } catch (writeError) {
        console.error(`[ppl-lint-grammar] ERROR: could not write artifacts: ${writeError.message}`);
      }
    }
    return 2;
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = run();
}

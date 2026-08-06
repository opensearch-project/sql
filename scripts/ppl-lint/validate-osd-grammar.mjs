/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HEADLESS = 'src/plugins/data/public/antlr/opensearch_ppl/headless_ppl_lint';
const CATALOGS = [
  'packages/osd-monaco/ppl-lint',
  'packages/osd-monaco/src/ppl/lint/catalog',
];
const MODULE_EXTENSIONS = ['', '.js', '.cjs', '.mjs', '.ts'];
const INDEX_FILES = ['index.js', 'index.cjs', 'index.mjs', 'index.ts'];
const SKIP_REASON = 'osd-headless-grammar-api-unavailable';
const OPTIONS = ['grammar', 'cases', 'target', 'osd-root', 'osd-sha', 'report', 'summary'];

class StructuralError extends Error {
  constructor(message, coverage) {
    super(message);
    this.coverage = coverage;
  }
}

function fail(message, coverage) {
  throw new StructuralError(message, coverage);
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

function moduleCandidates(root, name) {
  const base = path.join(root, name);
  return [
    ...MODULE_EXTENSIONS.map((extension) => `${base}${extension}`),
    ...INDEX_FILES.map((file) => path.join(base, file)),
  ];
}

function resolveModule(requireFromOsd, osdRoot, name) {
  for (const candidate of moduleCandidates(osdRoot, name)) {
    try {
      return requireFromOsd.resolve(candidate);
    } catch (error) {
      if (error?.code !== 'MODULE_NOT_FOUND') {
        fail(`Could not resolve OSD module ${name}: ${error.message}`);
      }
    }
  }
  return undefined;
}

function exportsOf(module) {
  return module?.default && typeof module.default === 'object'
    ? { ...module.default, ...module }
    : module;
}

async function importModule(requireFromOsd, resolved, name) {
  try {
    if (path.extname(resolved) === '.mjs') {
      return exportsOf(await import(pathToFileURL(resolved).href));
    }
    return exportsOf(requireFromOsd(resolved));
  } catch (error) {
    if (error?.code === 'ERR_REQUIRE_ESM') {
      try {
        return exportsOf(await import(pathToFileURL(resolved).href));
      } catch (importError) {
        fail(`Could not import OSD module ${name} from ${resolved}: ${importError.message}`);
      }
    }
    fail(`Could not import OSD module ${name} from ${resolved}: ${error.message}`);
  }
}

async function loadApi(osdRoot) {
  const requireFromOsd = createRequire(path.join(osdRoot, 'noop.js'));
  const resolvedHeadless = resolveModule(requireFromOsd, osdRoot, HEADLESS);
  if (!resolvedHeadless) return undefined;

  const headless = await importModule(requireFromOsd, resolvedHeadless, HEADLESS);
  if (
    typeof headless?.deserializeBundleOrThrow !== 'function' ||
    typeof headless?.lintQueryWithBundle !== 'function'
  ) {
    fail(`${HEADLESS} must export deserializeBundleOrThrow and lintQueryWithBundle.`);
  }

  let catalogModule;
  const missingCatalogs = [];
  for (const name of CATALOGS) {
    const resolved = resolveModule(requireFromOsd, osdRoot, name);
    if (!resolved) {
      missingCatalogs.push(name);
      continue;
    }
    const candidate = await importModule(requireFromOsd, resolved, name);
    if (typeof candidate?.getBundledCatalog !== 'function') {
      fail(`${name} must export getBundledCatalog.`);
    }
    catalogModule = candidate;
    break;
  }
  if (!catalogModule) {
    fail(`Could not resolve an OSD catalog module: ${missingCatalogs.join(', ')}.`);
  }

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
    catalogIds: [...catalogIds].sort(),
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

function sorted(values) {
  return [...new Set(values)].sort();
}

function makeCoverage(catalogIds, coveredRuleIds, excludedRules) {
  const catalogRuleIds = sorted(catalogIds);
  const covered = sorted(coveredRuleIds);
  const excluded = [...excludedRules].sort((left, right) => left.ruleId.localeCompare(right.ruleId));
  const catalog = new Set(catalogRuleIds);
  const excludedRuleIds = sorted(excluded.map((entry) => entry.ruleId));
  const excludedSet = new Set(excludedRuleIds);
  const requiredRuleIds = catalogRuleIds.filter((ruleId) => !excludedSet.has(ruleId));
  const required = new Set(requiredRuleIds);
  const missingRuleIds = requiredRuleIds.filter((ruleId) => !covered.includes(ruleId));
  const unexpectedRuleIds = sorted([
    ...covered.filter((ruleId) => !required.has(ruleId)),
    ...excludedRuleIds.filter((ruleId) => !catalog.has(ruleId)),
  ]);
  return {
    catalogRuleIds,
    requiredRuleIds,
    coveredRuleIds: covered,
    excludedRuleIds,
    excludedRules: excluded,
    missingRuleIds,
    unexpectedRuleIds,
    counts: {
      catalog: catalogRuleIds.length,
      required: requiredRuleIds.length,
      covered: covered.length,
      excluded: excludedRuleIds.length,
      missing: missingRuleIds.length,
      unexpected: unexpectedRuleIds.length,
    },
  };
}

function loadCases(file, catalogIds) {
  const document = object(readJson(file, 'grammar cases'), 'case document');
  if (document.schemaVersion !== 2) fail('case document schemaVersion must be 2.');
  if (!Array.isArray(document.excludedRules)) fail('case document excludedRules must be an array.');
  if (!Array.isArray(document.cases) || !document.cases.length) {
    fail('At least one grammar case is required.');
  }

  const exclusionIds = new Set();
  const duplicateExclusionIds = [];
  const excludedRules = document.excludedRules.map((rawExclusion, index) => {
    const exclusion = object(rawExclusion, `excludedRules[${index}]`);
    const ruleId = string(exclusion.ruleId, `excludedRules[${index}].ruleId`);
    const reason = string(exclusion.reason, `excludedRules[${index}].reason`);
    if (exclusionIds.has(ruleId)) duplicateExclusionIds.push(ruleId);
    exclusionIds.add(ruleId);
    return { ruleId, reason };
  });

  const caseIds = new Set();
  const duplicateCaseIds = [];
  const cases = document.cases.map((rawCase, index) => {
    const candidate = object(rawCase, `case[${index}]`);
    const id = string(candidate.id, `case[${index}].id`);
    const ruleId = string(candidate.ruleId, `case ${id}.ruleId`);
    if (caseIds.has(id)) duplicateCaseIds.push(id);
    caseIds.add(id);
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

  const coverage = makeCoverage(catalogIds, cases.map((entry) => entry.ruleId), excludedRules);
  const catalog = new Set(coverage.catalogRuleIds);
  const covered = new Set(coverage.coveredRuleIds);
  if (duplicateExclusionIds.length) {
    fail(
      `Duplicate exclusion rule ID(s): ${sorted(duplicateExclusionIds).join(', ')}.`,
      coverage
    );
  }
  if (duplicateCaseIds.length) {
    fail(`Duplicate case ID(s): ${sorted(duplicateCaseIds).join(', ')}.`, coverage);
  }
  const staleExclusions = coverage.excludedRuleIds.filter((ruleId) => !catalog.has(ruleId));
  if (staleExclusions.length) {
    fail(`Excluded rule(s) are absent from the OSD catalog: ${staleExclusions.join(', ')}.`, coverage);
  }
  const unknownCases = coverage.coveredRuleIds.filter((ruleId) => !catalog.has(ruleId));
  if (unknownCases.length) {
    fail(`Case rule(s) are absent from the OSD catalog: ${unknownCases.join(', ')}.`, coverage);
  }
  const overlap = coverage.excludedRuleIds.filter((ruleId) => covered.has(ruleId));
  if (overlap.length) {
    fail(`Rule(s) cannot be both covered and excluded: ${overlap.join(', ')}.`, coverage);
  }
  if (coverage.missingRuleIds.length || coverage.unexpectedRuleIds.length) {
    fail(
      `Case coverage must equal catalog rules minus exclusions; ` +
        `missing: ${coverage.missingRuleIds.join(', ') || 'none'}; ` +
        `unexpected: ${coverage.unexpectedRuleIds.join(', ') || 'none'}.`,
      coverage
    );
  }
  for (const ruleId of coverage.requiredRuleIds) {
    const kinds = new Set(cases.filter((entry) => entry.ruleId === ruleId).map((entry) => entry.kind));
    if (!kinds.has('trigger') || !kinds.has('control')) {
      fail(`Required rule ${JSON.stringify(ruleId)} must have trigger and control cases.`, coverage);
    }
  }
  return { cases, coverage };
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
      const lint = api.lint(grammarCase.query, grammar, contextFor(grammarCase, target, api.catalogIds));
      if (!Array.isArray(lint?.diagnostics)) throw new Error('lintQueryWithBundle returned no diagnostics array');
      result.actualCount = lint.diagnostics.filter((entry) => entry?.ruleId === grammarCase.ruleId).length;
      const unexpectedDiagnosticRuleIds = sorted(
        lint.diagnostics
          .filter((entry) => entry?.ruleId !== grammarCase.ruleId)
          .map((entry) => entry?.ruleId || '<missing>')
      );
      if (unexpectedDiagnosticRuleIds.length) {
        result.unexpectedDiagnosticRuleIds = unexpectedDiagnosticRuleIds;
        result.error =
          `lintQueryWithBundle returned diagnostics for non-target rule(s): ` +
          unexpectedDiagnosticRuleIds.join(', ');
      }
      result.status =
        result.actualCount === result.expectedCount && unexpectedDiagnosticRuleIds.length === 0
          ? 'passed'
          : 'failed';
    } catch (error) {
      result.actualCount = null;
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

function caseCounts(results) {
  const passed = results.filter((entry) => entry.status === 'passed').length;
  return { selected: results.length, passed, failed: results.length - passed };
}

function ruleCounts(coverage, cases = []) {
  const passed = coverage.requiredRuleIds.filter((ruleId) =>
    cases.length > 0 &&
    cases.filter((entry) => entry.ruleId === ruleId).every((entry) => entry.status === 'passed')
  ).length;
  return {
    catalog: coverage.counts.catalog,
    required: coverage.counts.required,
    excluded: coverage.counts.excluded,
    selected: coverage.counts.covered,
    passed,
    failed: cases.length > 0 ? coverage.counts.required - passed : 0,
  };
}

function makeReport(target, grammarHash, coverage, cases) {
  const failures = cases.filter((entry) => entry.status === 'failed').map((entry) => ({
    ruleId: entry.ruleId,
    caseId: entry.caseId,
    query: entry.query,
    expectedCount: entry.expectedCount,
    actualCount: entry.actualCount,
    ...(entry.unexpectedDiagnosticRuleIds
      ? { unexpectedDiagnosticRuleIds: entry.unexpectedDiagnosticRuleIds }
      : {}),
    ...(entry.error ? { error: entry.error } : {}),
  }));
  return {
    schemaVersion: 2,
    status: failures.length ? 'failed' : 'passed',
    sql: target.sql,
    osd: target.osd,
    manualOverride: target.manualOverride,
    releaseLineValidationBypassed: target.releaseLineValidationBypassed,
    grammarHash,
    coverage,
    rules: ruleCounts(coverage, cases),
    caseCounts: caseCounts(cases),
    cases,
    failures,
  };
}

function emptyCoverage() {
  return makeCoverage([], [], []);
}

function emptyReport(status, target, extra = {}, coverage = emptyCoverage()) {
  return {
    schemaVersion: 2,
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
    coverage,
    rules: ruleCounts(coverage),
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
  lines.push(
    `- Rules: ${report.rules.required} required, ${report.rules.selected} covered, ` +
      `${report.rules.excluded} excluded, ${report.rules.passed} passed, ${report.rules.failed} failed`
  );
  if (report.coverage.missingRuleIds.length) {
    lines.push(`- Missing rules: ${report.coverage.missingRuleIds.map((id) => `\`${id}\``).join(', ')}`);
  }
  if (report.coverage.unexpectedRuleIds.length) {
    lines.push(`- Unexpected rules: ${report.coverage.unexpectedRuleIds.map((id) => `\`${id}\``).join(', ')}`);
  }
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

export async function run(argv = process.argv.slice(2)) {
  let args;
  let target;
  let coverage;
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
    const api = await loadApi(osdRoot);
    if (!api) {
      writeOutputs(args, emptyReport('skipped', target, { skipReason: SKIP_REASON }));
      return 0;
    }
    const { bundle, grammar } = loadGrammar(args.grammar, api.deserialize);
    const loadedCases = loadCases(args.cases, api.catalogIds);
    coverage = loadedCases.coverage;
    const report = makeReport(
      target,
      bundle.grammarHash,
      coverage,
      executeCases(api, grammar, loadedCases.cases, target)
    );
    writeOutputs(args, report);
    return report.status === 'passed' ? 0 : 1;
  } catch (error) {
    coverage = error?.coverage || coverage;
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[ppl-lint-grammar] ERROR: ${message}`);
    if (args?.report && args?.summary) {
      try {
        writeOutputs(args, emptyReport('error', target, { error: message }, coverage));
      } catch (writeError) {
        console.error(`[ppl-lint-grammar] ERROR: could not write artifacts: ${writeError.message}`);
      }
    }
    return 2;
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = await run();
}

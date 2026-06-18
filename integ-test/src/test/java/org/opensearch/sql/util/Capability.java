/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

/**
 * Backend-agnostic registry of execution capabilities a test may require. Each constant names a
 * behavior (nested fields, document mutation, stable head ordering, ...) and carries the reason it
 * is unavailable on a backend that lacks it — currently the analytics-engine route. Tests declare
 * the capability they need via {@code BackendCapabilities.requireCapability(...)} or the {@link
 * RequiresCapability} annotation rather than naming a backend.
 *
 * <p>Keeping every reason here makes the full set of route gaps greppable in one place — both for
 * humans tracking what still needs fixing and as a single block of context to hand an agent for
 * bulk triage.
 */
public enum Capability {
  /**
   * The parquet/composite store has no nested-document support, so nested fields are stripped from
   * the dataset at load (#5541) and queries that reference them resolve against fields that don't
   * exist on this route.
   */
  NESTED_FIELDS(
      "Nested-field queries can't run on the analytics-engine route: the parquet/composite store"
          + " has no nested-document support, so nested fields are stripped from the dataset at"
          + " load (#5541)."),

  /**
   * Parquet-backed scans surface only mapped document fields, so the {@code _id} metadata field is
   * not exposed on this route.
   */
  ID_METADATA(
      "The analytics-engine route doesn't expose the _id metadata field (parquet-backed scans"
          + " surface only mapped document fields)."),

  /**
   * The composite dataformat's dynamic mapping gives string fields a {@code text} mapping without
   * the {@code .keyword} sub-field that standard OpenSearch adds, so exact equality ({@code =} /
   * {@code ==}) on a dynamically-mapped string silently returns no rows on the DataFusion scan.
   * Explicitly mapped {@code text}+{@code keyword} fields are unaffected.
   */
  DYNAMIC_STRING_NO_KEYWORD(
      "Exact equality (= / ==) on a dynamically-mapped string field returns no rows on the"
          + " analytics-engine route: the composite dataformat's dynamic mapping omits the .keyword"
          + " sub-field that standard OpenSearch adds, and the DataFusion scan can't match on an"
          + " analyzed text field."),

  /**
   * Exact equality ({@code =} / {@code ==}) on an explicitly {@code text}-mapped field (no {@code
   * .keyword} sub-field) returns no rows on the analytics-engine route — the DataFusion scan can't
   * match on an analyzed text field. The sibling of {@link #DYNAMIC_STRING_NO_KEYWORD} for fields
   * that are mapped {@code text} on purpose rather than by dynamic mapping. Verified directly:
   * {@code where department = 'DATA'} returns no rows while {@code like(department, 'DATA')} and
   * keyword-field equality both work.
   */
  TEXT_FIELD_EXACT_MATCH(
      "Exact equality (= / ==) on an explicitly text-mapped field (no .keyword sub-field) returns"
          + " no rows on the analytics-engine route: the DataFusion scan can't match on an analyzed"
          + " text field. Use like() or a keyword field instead."),

  /**
   * The analytics-engine storage path ({@code DataFormatAwareEngine}) does not support in-place
   * document mutation, so tests that seed state via raw {@code PUT}+{@code DELETE} can't run on
   * this route.
   */
  DOC_MUTATION(
      "Test mutates docs via PUT+DELETE, which DataFormatAwareEngine (analytics-engine storage"
          + " path) does not support."),

  /**
   * When every {@code multisearch} subsearch reads the same index, the analytics-engine route
   * applies the first subsearch's filter to all of them (each keeps its own {@code eval} label), so
   * later subsearches silently return the first subsearch's rows. Produces wrong counts/duplication
   * — the route can't be asserted against. Reproduces single-shard.
   */
  MULTISEARCH_SAME_INDEX_CONFLATION(
      "multisearch with same-index subsearches conflates on the analytics-engine route: every"
          + " subsearch executes the first subsearch's filter, so counts/rows are wrong."),

  /**
   * A {@code multisearch} over heterogeneous indices returns the merged columns in a different
   * order than the v2/Calcite path (e.g. trailing fields swapped), so row-order-sensitive
   * assertions diverge even though the values are correct.
   */
  MULTISEARCH_COLUMN_ORDER(
      "multisearch over different indices returns merged columns in a different order on the"
          + " analytics-engine route than the v2/Calcite path."),

  /**
   * Binning a time field then grouping by it ({@code bin <timefield> bins=N | stats ... by
   * <timefield>}) diverges on the analytics-engine route: the date-histogram bucket column comes
   * back typed {@code string} rather than {@code timestamp}, and the route produces a different
   * bucket set (different auto-histogram span / empty buckets not filtered) so the row counts don't
   * match the v2/Calcite path.
   */
  BIN_TIME_FIELD_BUCKETING(
      "bin on a time field then grouping by it diverges on the analytics-engine route: the bucket"
          + " column is typed string (not timestamp) and the bucket set differs from the v2/Calcite"
          + " path."),

  /**
   * {@code COALESCE} over operands that are all untyped NULL (e.g. {@code coalesce(field1, field2,
   * field3)} where every field is missing from the mapping) is rejected by the analytics-engine
   * capability registry with {@code No backend supports scalar function [COALESCE] among
   * [datafusion]}. The v2/Calcite path returns an {@code undefined}-typed null instead (#5175).
   */
  COALESCE_ALL_NULL_OPERANDS(
      "COALESCE over all-untyped-null operands is unsupported on the analytics-engine route (the"
          + " capability registry rejects it); the v2/Calcite path returns an undefined-typed"
          + " null."),

  /**
   * A {@code head N} without a stable {@code sort} returns a non-deterministic row set on the
   * analytics-engine route — raw-{@code PUT} docs land in a separate segment and can sort ahead of
   * the bulk-loaded docs. Adding a {@code sort} fixes it only when the sort key is unique over the
   * head window; when ties fall back to a nullable key, null placement diverges between the
   * v2/Calcite and analytics routes, so the row set still differs.
   */
  HEAD_WITHOUT_STABLE_SORT(
      "head N without a sort on a key that is unique over the head window is non-deterministic on"
          + " the analytics-engine route, and a nullable tiebreak orders nulls differently than the"
          + " v2/Calcite path."),

  /**
   * The {@code subsearch.maxout} cap on an {@code in}-subquery is lowered as a {@code LIMIT} on the
   * right-hand side of the semi-join ({@code LogicalSystemLimit(fetch=N, type=SUBSEARCH_MAXOUT)}).
   * The analytics-engine route does not honor that LIMIT, so the subsearch returns all rows
   * regardless of the cap. Verified: with {@code subsearch.maxout=1} an {@code id in [...]}
   * subquery still returns every matching row.
   */
  SUBSEARCH_MAXOUT_IN_SUBQUERY(
      "subsearch.maxout is not honored on the analytics-engine route: the LIMIT lowered onto the"
          + " in-subquery semi-join's right side is dropped, so the subsearch returns all rows"
          + " regardless of the cap."),

  /**
   * Querying an {@code object}/struct parent field directly (e.g. {@code isnull(aws)} where {@code
   * aws} is an {@code object}) fails on the analytics-engine route with {@code FIELD_NOT_FOUND}.
   * The route flattens objects into dotted leaf columns — {@code aws.cloudwatch.log_group} scans
   * fine — but the struct parent is not exposed as a queryable column. Distinct from {@link
   * #NESTED_FIELDS}: {@code object} parents survive in the OpenSearch mapping (they aren't stripped
   * at load) yet still can't be referenced as a whole.
   */
  STRUCT_PARENT_FIELD(
      "Querying an object/struct parent field directly is unsupported on the analytics-engine"
          + " route: objects are flattened to dotted leaf columns and the parent resolves to"
          + " FIELD_NOT_FOUND."),

  /**
   * {@code concat()} over a NULL argument diverges: the analytics-engine route (DataFusion) treats
   * NULL as an empty string (e.g. {@code concat('H', null)} = {@code 'H'}), whereas the v2/Calcite
   * engine propagates NULL ({@code concat('H', null)} = {@code null}). Any expression that depends
   * on the NULL-propagating behavior over a possibly-null operand diverges.
   */
  CONCAT_NULL_AS_EMPTY(
      "concat() treats a NULL argument as an empty string on the analytics-engine route (DataFusion"
          + " semantics), whereas the v2/Calcite engine propagates NULL."),

  /**
   * {@code earliest('now', <ts>)} / {@code latest('now', <ts>)} where {@code <ts>} is {@code
   * utc_timestamp()} diverge: on the analytics-engine route the relative-time {@code 'now'} and
   * {@code utc_timestamp()} resolve to the same instant (so {@code earliest('now', now)} is {@code
   * true}), whereas on the v2/Calcite path they differ (it is {@code false}) — a clock-source
   * divergence between the relative-time evaluation and {@code utc_timestamp()}.
   */
  EARLIEST_LATEST_NOW_CLOCK(
      "earliest/latest with relative-time 'now' against utc_timestamp() diverges on the"
          + " analytics-engine route: 'now' and utc_timestamp() resolve to the same instant"
          + " (earliest('now', now) is true), but differ on the v2/Calcite path (false)."),

  /**
   * eval {@code max(...)} / {@code min(...)} over operands that mix numeric and string types (e.g.
   * {@code max(14, age, 'Fred', holdersName)}) throws {@code SqlValidatorException: Cannot infer
   * return type for GREATEST} on the analytics-engine route. The SCALAR_MAX/SCALAR_MIN UDF is
   * converted to a DataFusion {@code GREATEST}/{@code LEAST} during Substrait conversion, and that
   * operator can't unify heterogeneous operand types. The v2/Calcite path applies OpenSearch's
   * mixed-type comparison (strings sort above numbers) and returns a result.
   */
  EVAL_MAX_MIN_MIXED_TYPES(
      "eval max()/min() over mixed numeric+string operands throws 'Cannot infer return type for"
          + " GREATEST' on the analytics-engine route: the DataFusion GREATEST/LEAST can't unify"
          + " heterogeneous operand types."),

  /**
   * eval {@code max(...)} / {@code min(...)} over all-integer operands reports the result column as
   * {@code bigint} on the analytics-engine route — DataFusion widens integer results to Int64 —
   * whereas the v2/Calcite path reports {@code int} when the selected value fits in an int. The
   * values match; only the schema's integer width differs.
   */
  EVAL_MAX_MIN_INT_WIDENING(
      "eval max()/min() over integer operands reports the result column as bigint on the"
          + " analytics-engine route (DataFusion widens integers to Int64), whereas the v2/Calcite"
          + " path reports int."),

  /**
   * Lucene full-text relevance functions ({@code match}, {@code multi_match}, {@code query_string},
   * {@code simple_query_string}, …) are unsupported on the analytics-engine route — DataFusion has
   * no relevance scorer, so a query that filters on one returns no rows.
   */
  FULLTEXT_RELEVANCE_FUNC(
      "Full-text relevance functions (match/multi_match/query_string/simple_query_string) are"
          + " unsupported on the analytics-engine route: DataFusion has no relevance scorer, so the"
          + " filter returns no rows."),

  /**
   * LIKE is case-insensitive on the analytics-engine route (DataFusion), whereas the v2/Calcite
   * path treats {@code LIKE} as case-sensitive (only {@code ILIKE} is case-insensitive).
   */
  LIKE_CASE_SENSITIVITY(
      "LIKE is case-insensitive on the analytics-engine route (DataFusion), whereas the v2/Calcite"
          + " path treats LIKE as case-sensitive."),

  /**
   * {@code appendpipe [subpipe]} drops the main pipeline's rows on the analytics-engine route: the
   * subpipe's filter is applied to the main result instead of its output being appended to it, so
   * the original rows are lost (e.g. {@code stats ... | appendpipe [where gender='F']} returns only
   * the filtered F rows, not the originals plus the filtered copy).
   */
  APPENDPIPE_MAIN_RESULT_DROPPED(
      "appendpipe drops the main pipeline's rows on the analytics-engine route: the subpipe filter"
          + " is applied to the main result instead of appended, so the originals are lost."),

  /**
   * Higher-order array functions that take a lambda ({@code transform}/{@code mvmap}, {@code
   * reduce}, {@code filter}, {@code exists}, {@code forall}) are unsupported on the
   * analytics-engine route: the capability registry rejects them ({@code No backend supports scalar
   * function [...] among [lucene, datafusion]}) since the backends can't execute a PPL lambda.
   */
  ARRAY_HIGHER_ORDER_FUNC(
      "Higher-order array functions (transform/mvmap, reduce, filter, exists, forall) are"
          + " unsupported on the analytics-engine route: the backends can't execute a PPL lambda."),

  /**
   * {@code scaled_float} fields are reported as {@code bigint} on the analytics-engine route
   * (DataFusion stores the underlying scaled long) rather than {@code double} as on v2/Calcite.
   */
  SCALED_FLOAT_TYPE(
      "scaled_float is reported as bigint on the analytics-engine route (DataFusion stores the"
          + " scaled long), whereas the v2/Calcite path reports double."),

  /**
   * Coercing an empty string to a numeric field yields {@code null} on the analytics-engine route,
   * whereas the v2/Calcite path coerces it to {@code 0}.
   */
  STRING_TO_NUMERIC_COERCION(
      "Coercing an empty/invalid string to a numeric field yields null on the analytics-engine"
          + " route, whereas the v2/Calcite path coerces it to 0."),

  /**
   * A wildcard/alias source whose member indices map the same field to incompatible types (e.g.
   * {@code text} in one, {@code boolean} in another) is rejected on the analytics-engine route
   * ({@code resolves to indices with incompatible field types}); the v2/Calcite path coerces.
   */
  CROSS_INDEX_INCOMPATIBLE_TYPES(
      "A wildcard/alias source with incompatible field types across member indices is rejected on"
          + " the analytics-engine route, whereas the v2/Calcite path coerces."),

  /**
   * The {@code REGEXP} filter operator throws a backend NullPointerException on the
   * analytics-engine route.
   */
  REGEXP_FILTER(
      "The REGEXP filter operator throws a backend NullPointerException on the analytics-engine"
          + " route."),

  /**
   * {@code mvcombine} lowers to an {@code ARRAY_AGG} aggregate the analytics-engine backend doesn't
   * register ({@code No enum constant ... AggregateFunction.ARRAY_AGG}).
   */
  MVCOMBINE_ARRAY_AGG(
      "mvcombine lowers to ARRAY_AGG, which the analytics-engine backend does not support (no"
          + " AggregateFunction.ARRAY_AGG enum constant)."),

  /**
   * {@code addtotals} crashes the DataFusion backend with a join panic (out-of-range slice index)
   * on the analytics-engine route.
   */
  ADDTOTALS_JOIN_PANIC(
      "addtotals crashes the DataFusion backend with a join panic (out-of-range slice index) on the"
          + " analytics-engine route."),

  /**
   * {@code percentile}/{@code median} is approximate on the analytics-engine route (DataFusion's
   * approx percentile) but exact on the v2/Calcite path, so percentile values, null-bucket rows,
   * and by-span groupings diverge.
   */
  PERCENTILE_APPROXIMATE(
      "percentile/median is approximate on the analytics-engine route (DataFusion) but exact on the"
          + " v2/Calcite path, so the values diverge."),

  /**
   * Arithmetic over {@code float}/{@code half_float}-typed fields keeps 32-bit float precision on
   * the analytics-engine route (DataFusion), whereas the v2/Calcite path widens to double, so the
   * least-significant digits diverge (e.g. 0.2 vs 0.19999981).
   */
  FLOAT_ARITHMETIC_PRECISION(
      "Arithmetic over float/half_float fields keeps 32-bit precision on the analytics-engine route"
          + " (DataFusion) but widens to double on the v2/Calcite path, so the values diverge in"
          + " the least-significant digits."),

  /**
   * Datetime formatting functions ({@code date_format}, {@code strftime}) render some tokens /
   * sub-second precision differently on the analytics-engine route than on the v2/Calcite path.
   */
  DATETIME_FORMAT_RENDERING(
      "date_format/strftime render some format tokens and sub-second precision differently on the"
          + " analytics-engine route than the v2/Calcite path."),

  /**
   * {@code json_set}/{@code json_delete} with a {@code $}-prefixed path ({@code $.key}) is a no-op
   * on the analytics-engine route (the JSON UDF doesn't strip the {@code $} prefix), whereas the
   * v2/Calcite path applies the modification.
   */
  JSON_DOLLAR_PATH(
      "json_set/json_delete with a $-prefixed path is a no-op on the analytics-engine route (the"
          + " JSON UDF doesn't handle the $ prefix), whereas the v2/Calcite path applies it."),

  /**
   * A dataset whose document has a multi-value array for a scalar-mapped field can't be bulk-loaded
   * into the parquet/composite store ({@code Cannot accept multiple values for field ...}), so
   * tests reading that dataset fail at setup on the analytics-engine route.
   */
  MULTI_VALUE_FIELD_LOAD(
      "A multi-value array for a scalar-mapped field can't be bulk-loaded into the parquet store on"
          + " the analytics-engine route, so the dataset fails to load."),

  /**
   * {@code dedup} returns a different/non-deterministic row set on the analytics-engine route — the
   * engine merges per-fragment batches without a stable tiebreaker, so which duplicate survives
   * (and {@code CONSECUTIVE=true} behavior) diverges from the v2/Calcite path.
   */
  DEDUP_NONDETERMINISTIC(
      "dedup returns a different row set on the analytics-engine route: per-fragment merge order"
          + " has no stable tiebreaker, so the surviving duplicate (and CONSECUTIVE behavior)"
          + " diverges."),

  /**
   * {@code union}/{@code multisearch} over subsearches that read the same index conflates on the
   * analytics-engine route: a delegated predicate from one branch leaks onto the co-located shard
   * fragment and is applied to all branches, so counts/rows are wrong. Same root cause as {@link
   * #MULTISEARCH_SAME_INDEX_CONFLATION} / {@link #APPENDPIPE_MAIN_RESULT_DROPPED}.
   */
  SAME_INDEX_UNION_CONFLATION(
      "union over same-index subsearches conflates on the analytics-engine route: a delegated"
          + " predicate from one branch leaks across the co-located shard fragment, so counts/rows"
          + " are wrong."),

  /**
   * A wildcard projection/rename ({@code rename * as ...}, {@code fields *}) returns columns in a
   * different order on the analytics-engine route (e.g. not mapping order) than the v2/Calcite
   * path, so row-position-sensitive assertions diverge even though the values are correct.
   */
  WILDCARD_COLUMN_ORDER(
      "A wildcard projection/rename returns columns in a different order on the analytics-engine"
          + " route than the v2/Calcite path."),

  /**
   * {@code unix_timestamp()} over a timestamp string with sub-second precision drops the fractional
   * seconds on the analytics-engine route (e.g. {@code unix_timestamp('1984-06-06
   * 12:00:00.123456')} returns {@code 455371200} instead of {@code 455371200.123456}), whereas the
   * v2/Calcite path preserves them.
   */
  UNIX_TIMESTAMP_SUBSECOND(
      "unix_timestamp() drops sub-second precision on the analytics-engine route (returns whole"
          + " seconds), whereas the v2/Calcite path preserves the fractional seconds.");

  private final String reason;

  Capability(String reason) {
    this.reason = reason;
  }

  /** Human-readable explanation surfaced as the JUnit skip message. */
  public String reason() {
    return reason;
  }
}

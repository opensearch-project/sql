/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Operator table mapping ClickHouse function names to Calcite equivalents. Implements
 * SqlOperatorTable so it can be chained with Calcite's default table during validation.
 *
 * <p>Function mappings organized by translation type:
 *
 * <ul>
 *   <li>Simple renames: now() → CURRENT_TIMESTAMP, today() → CURRENT_DATE, groupArray() →
 *       ARRAY_AGG
 *   <li>CAST rewrites: toDateTime → CAST AS TIMESTAMP, toDate → CAST AS DATE, etc.
 *   <li>Aggregate rewrites: uniq/uniqExact → COUNT(DISTINCT), count() → COUNT(*)
 *   <li>CASE WHEN rewrites: if → CASE WHEN, multiIf → CASE WHEN
 *   <li>Date truncation: toStartOfHour → DATE_TRUNC('HOUR', col), etc.
 *   <li>Special: quantile → PERCENTILE_CONT, formatDateTime → DATE_FORMAT
 * </ul>
 */
public class ClickHouseOperatorTable implements SqlOperatorTable {

  public static final ClickHouseOperatorTable INSTANCE = new ClickHouseOperatorTable();

  /** Map from lowercase ClickHouse function name to Calcite operator. */
  private final Map<String, SqlOperator> operatorMap = new HashMap<>();

  /**
   * Thread-safe cache for resolved operator lookups, keyed by normalized (uppercase) function name.
   * Since the set of registered functions is finite and keys are normalized, this cache is naturally
   * bounded — it can hold at most one entry per registered function name.
   */
  private final ConcurrentHashMap<String, List<SqlOperator>> lookupCache =
      new ConcurrentHashMap<>();

  private ClickHouseOperatorTable() {
    registerTimeBucketingFunctions();
    registerTypeConversionFunctions();
    registerAggregateFunctions();
    registerConditionalFunctions();
    registerSpecialFunctions();
  }

  /**
   * Register time-bucketing functions that translate to DATE_TRUNC. toStartOfHour(col) →
   * DATE_TRUNC('HOUR', col), toStartOfDay(col) → DATE_TRUNC('DAY', col), etc.
   *
   * <p>toStartOfInterval(col, INTERVAL N unit) is also registered but takes 2 args.
   *
   * <p><b>Semantic difference — timezone handling:</b> ClickHouse {@code toStartOfInterval} and
   * related functions use the <em>server timezone</em> by default when no explicit timezone argument
   * is provided. Calcite {@code DATE_TRUNC} uses the <em>session timezone</em>. This can produce
   * different results when the server and session timezones differ. Callers should be aware that
   * time-bucket boundaries may shift depending on the timezone configuration.
   *
   * <p><b>Implicit type promotion (Req 13.4):</b> ClickHouse time-bucketing functions accept
   * strings and integers in addition to DateTime/Date types, performing implicit conversion to
   * timestamp. For example, {@code toStartOfHour('2024-01-01 12:34:56')} is valid in ClickHouse.
   * Calcite's strict type checking with {@code SqlTypeFamily.TIMESTAMP} would reject such inputs.
   * To achieve equivalent behavior, these functions use {@code OperandTypes.ANY} to accept any
   * input type, relying on Calcite's type coercion to insert an implicit CAST to TIMESTAMP during
   * validation when the input is not already a timestamp type.
   */
  private void registerTimeBucketingFunctions() {
    // toStartOfInterval takes 2 args: column and interval
    // Semantic note: ClickHouse defaults to server timezone; Calcite DATE_TRUNC uses session tz.
    // Implicit promotion (Req 13.4): first arg accepts ANY type — ClickHouse implicitly converts
    // strings/integers to DateTime. Calcite's type coercion inserts CAST(arg AS TIMESTAMP).
    register(
        "tostartofinterval",
        createFunction(
            "toStartOfInterval",
            ReturnTypes.TIMESTAMP_NULLABLE,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE));

    // Single-arg time-bucketing functions
    // Implicit promotion (Req 13.4): accept ANY type to match ClickHouse's implicit
    // string/integer-to-timestamp conversion. Calcite inserts CAST during validation.
    register(
        "tostartofhour",
        createFunction(
            "toStartOfHour",
            ReturnTypes.TIMESTAMP_NULLABLE,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE));

    register(
        "tostartofday",
        createFunction(
            "toStartOfDay",
            ReturnTypes.TIMESTAMP_NULLABLE,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE));

    register(
        "tostartofminute",
        createFunction(
            "toStartOfMinute",
            ReturnTypes.TIMESTAMP_NULLABLE,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE));

    register(
        "tostartofweek",
        createFunction(
            "toStartOfWeek",
            ReturnTypes.DATE_NULLABLE,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE));

    register(
        "tostartofmonth",
        createFunction(
            "toStartOfMonth",
            ReturnTypes.DATE_NULLABLE,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE));
  }

  /**
   * Register type-conversion functions that translate to CAST expressions. toDateTime(x) → CAST(x
   * AS TIMESTAMP), toDate(x) → CAST(x AS DATE), etc.
   *
   * <p><b>Semantic difference — null handling:</b> ClickHouse type-conversion functions like
   * {@code toDateTime} return {@code NULL} for unparseable or invalid input strings (e.g.,
   * {@code toDateTime('not-a-date')} → NULL). Calcite's {@code CAST} may throw a runtime exception
   * for the same input. Callers should handle NULL inputs explicitly or pre-validate data to avoid
   * unexpected errors.
   *
   * <p><b>Semantic difference — unsigned types:</b> ClickHouse distinguishes unsigned integer types
   * ({@code toUInt32}) from signed types ({@code toInt32}). Calcite has no unsigned integer types,
   * so {@code toUInt32} is mapped to {@code CAST(x AS INTEGER)} (signed). Values exceeding
   * {@code Integer.MAX_VALUE} in the unsigned range will overflow or produce incorrect results.
   *
   * <p><b>Implicit type promotion (Req 13.4):</b> These functions already use
   * {@code OperandTypes.ANY} to accept any input type, matching ClickHouse's behavior where
   * type-conversion functions accept strings, numbers, dates, and other types interchangeably.
   * No additional explicit CAST is needed — the functions themselves ARE the explicit CAST
   * translation (e.g., {@code toDateTime(x)} → {@code CAST(x AS TIMESTAMP)}).
   */
  private void registerTypeConversionFunctions() {
    // Semantic note: ClickHouse toDateTime returns NULL for unparseable strings;
    // Calcite CAST(x AS TIMESTAMP) may throw on invalid input.
    register(
        "todatetime",
        createFunction(
            "toDateTime",
            ReturnTypes.explicit(SqlTypeName.TIMESTAMP),
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM));

    register(
        "todate",
        createFunction(
            "toDate",
            ReturnTypes.explicit(SqlTypeName.DATE),
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM));

    register(
        "tostring",
        createFunction(
            "toString",
            ReturnTypes.explicit(SqlTypeName.VARCHAR),
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM));

    register(
        "touint32",
        createFunction(
            "toUInt32",
            ReturnTypes.explicit(SqlTypeName.INTEGER),
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM));

    register(
        "toint32",
        createFunction(
            "toInt32",
            ReturnTypes.explicit(SqlTypeName.INTEGER),
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM));

    register(
        "toint64",
        createFunction(
            "toInt64",
            ReturnTypes.explicit(SqlTypeName.BIGINT),
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM));

    register(
        "tofloat64",
        createFunction(
            "toFloat64",
            ReturnTypes.explicit(SqlTypeName.DOUBLE),
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM));

    register(
        "tofloat32",
        createFunction(
            "toFloat32",
            ReturnTypes.explicit(SqlTypeName.FLOAT),
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM));
  }

  /**
   * Register aggregate functions. uniq(x)/uniqExact(x) → COUNT(DISTINCT x), groupArray(x) →
   * ARRAY_AGG(x), count() with no args → COUNT(*).
   *
   * <p><b>Semantic difference — approximation:</b> ClickHouse {@code uniq(x)} uses a HyperLogLog
   * approximation algorithm for cardinality estimation, which is fast but may return slightly
   * inaccurate results for large cardinalities (typical error rate ~2%). The translated
   * {@code COUNT(DISTINCT x)} is exact. {@code uniqExact(x)} is exact in ClickHouse and maps
   * cleanly to {@code COUNT(DISTINCT x)}, so no semantic gap exists for that variant.
   *
   * <p><b>Semantic difference — groupArray ordering:</b> ClickHouse {@code groupArray(x)}
   * preserves insertion order within each group. Calcite {@code ARRAY_AGG(x)} order is
   * implementation-defined unless an explicit {@code ORDER BY} is specified within the aggregate.
   *
   * <p><b>Implicit type promotion (Req 13.4):</b> These aggregate functions delegate to Calcite's
   * built-in {@code COUNT} and {@code ARRAY_AGG} operators, which already accept any input type
   * through their own operand type checking. No additional explicit CAST is needed.
   */
  private void registerAggregateFunctions() {
    // uniq and uniqExact → COUNT (will be used with DISTINCT flag during planning)
    // Semantic note: uniq uses HyperLogLog (~2% error); COUNT(DISTINCT) is exact.
    // uniqExact is exact in ClickHouse, so the mapping is semantically equivalent.
    SqlOperator countOp = SqlStdOperatorTable.COUNT;
    register("uniq", countOp);
    register("uniqexact", countOp);

    // groupArray → ARRAY_AGG
    register("grouparray", SqlLibraryOperators.ARRAY_AGG);

    // count() with no args → COUNT(*) — register standard COUNT
    // Calcite's COUNT already handles the no-args case as COUNT(*)
    register("count", countOp);
  }

  /**
   * Register conditional functions. if(cond, then, else) → CASE WHEN cond THEN then ELSE else END
   * multiIf(c1, v1, c2, v2, ..., default) → CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ... ELSE default
   * END
   *
   * <p><b>Semantic difference — null in conditions:</b> ClickHouse {@code if()} treats NULL
   * conditions as false (the else branch is taken). Calcite {@code CASE WHEN} also treats NULL
   * conditions as not-true, so the mapping is semantically equivalent for NULL conditions.
   *
   * <p><b>Implicit type promotion (Req 13.4):</b> ClickHouse {@code if()} and {@code multiIf()}
   * perform implicit type promotion across the then/else branches (e.g., Int32 and Float64 are
   * promoted to Float64). Calcite uses its own type coercion rules via {@code LEAST_RESTRICTIVE},
   * which handles most numeric promotion cases equivalently. The condition argument uses
   * {@code SqlTypeFamily.BOOLEAN} while value branches use {@code SqlTypeFamily.ANY} to allow
   * mixed types that Calcite will coerce. For {@code multiIf}, {@code OperandTypes.VARIADIC}
   * accepts any combination of types. No additional explicit CAST is needed because Calcite's
   * {@code LEAST_RESTRICTIVE} return type inference already performs the equivalent promotion.
   * Edge cases involving mixed numeric and string types may differ.
   */
  private void registerConditionalFunctions() {
    // ClickHouse if(cond, then_val, else_val) — 3 args
    register(
        "if",
        createFunction(
            "if",
            ReturnTypes.LEAST_RESTRICTIVE,
            OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
            SqlFunctionCategory.SYSTEM));

    // ClickHouse multiIf(c1, v1, c2, v2, ..., default) — variadic
    register(
        "multiif",
        createFunction(
            "multiIf",
            ReturnTypes.LEAST_RESTRICTIVE,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.SYSTEM));
  }

  /**
   * Register special functions: quantile → PERCENTILE_CONT, formatDateTime → DATE_FORMAT, now() →
   * CURRENT_TIMESTAMP, today() → CURRENT_DATE.
   *
   * <p><b>Semantic difference — quantile interpolation:</b> ClickHouse {@code quantile(level)(x)}
   * uses a sampling-based approximation (t-digest or similar) that may return slightly different
   * results than Calcite's {@code PERCENTILE_CONT}, which uses linear interpolation on the exact
   * sorted dataset. Results may diverge for small datasets or extreme quantile levels (near 0 or
   * 1).
   *
   * <p><b>Semantic difference — formatDateTime patterns:</b> ClickHouse {@code formatDateTime}
   * uses its own format specifiers (e.g., {@code %Y-%m-%d %H:%M:%S}) which differ from standard
   * Java/SQL format patterns. The translated {@code DATE_FORMAT} must receive ClickHouse-style
   * format strings; no automatic pattern conversion is performed.
   *
   * <p><b>Semantic difference — now() precision:</b> ClickHouse {@code now()} returns a
   * second-precision DateTime. Calcite {@code CURRENT_TIMESTAMP} may return higher precision
   * (milliseconds or microseconds) depending on the engine. Similarly, {@code today()} in
   * ClickHouse returns a Date type, while Calcite {@code CURRENT_DATE} is equivalent.
   *
   * <p><b>Implicit type promotion (Req 13.4):</b> ClickHouse {@code formatDateTime} accepts
   * strings and integers as the first argument, implicitly converting them to DateTime. The
   * first operand uses {@code ANY} type to match this behavior. ClickHouse {@code quantile}
   * also accepts string arguments that look like numbers; both operands use {@code ANY} type.
   */
  private void registerSpecialFunctions() {
    // quantile(level)(expr) — registered as a function taking 2 args (level, expr)
    // Implicit promotion (Req 13.4): ClickHouse accepts string args that look like numbers;
    // use ANY to allow Calcite's type coercion to insert CAST where needed.
    register(
        "quantile",
        createFunction(
            "quantile",
            ReturnTypes.DOUBLE_NULLABLE,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.NUMERIC));

    // formatDateTime(datetime, format_string) → DATE_FORMAT
    // Implicit promotion (Req 13.4): first arg accepts ANY type — ClickHouse implicitly converts
    // strings/integers to DateTime. Calcite's type coercion inserts CAST(arg AS TIMESTAMP).
    register(
        "formatdatetime",
        createFunction(
            "formatDateTime",
            ReturnTypes.VARCHAR_2000,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.TIMEDATE));

    // now() → CURRENT_TIMESTAMP
    register(
        "now",
        createFunction(
            "now", ReturnTypes.TIMESTAMP, OperandTypes.NILADIC, SqlFunctionCategory.TIMEDATE));

    // today() → CURRENT_DATE
    register(
        "today",
        createFunction(
            "today", ReturnTypes.DATE, OperandTypes.NILADIC, SqlFunctionCategory.TIMEDATE));
  }

  /**
   * Register an operator under a lowercase key.
   *
   * @param name the ClickHouse function name (will be lowercased)
   * @param operator the Calcite operator to map to
   */
  private void register(String name, SqlOperator operator) {
    operatorMap.put(name.toLowerCase(Locale.ROOT), operator);
  }

  /**
   * Create a SqlFunction with the given properties.
   *
   * @param name the function name
   * @param returnType the return type inference
   * @param operandTypes the operand type checker
   * @param category the function category
   * @return a new SqlFunction
   */
  private static SqlFunction createFunction(
      String name,
      org.apache.calcite.sql.type.SqlReturnTypeInference returnType,
      org.apache.calcite.sql.type.SqlOperandTypeChecker operandTypes,
      SqlFunctionCategory category) {
    return new SqlFunction(
        name, SqlKind.OTHER_FUNCTION, returnType, InferTypes.FIRST_KNOWN, operandTypes, category);
  }

  @Override
  public void lookupOperatorOverloads(
      SqlIdentifier opName,
      @Nullable SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher) {
    if (opName.isSimple()) {
      // Normalize to uppercase for case-insensitive, bounded cache keys
      String cacheKey = opName.getSimple().toUpperCase(Locale.ROOT);
      List<SqlOperator> cached =
          lookupCache.computeIfAbsent(
              cacheKey,
              key -> {
                String lowerName = key.toLowerCase(Locale.ROOT);
                SqlOperator op = operatorMap.get(lowerName);
                return op != null
                    ? Collections.singletonList(op)
                    : Collections.emptyList();
              });
      operatorList.addAll(cached);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return new ArrayList<>(operatorMap.values());
  }

  /**
   * Returns the set of registered ClickHouse function names (lowercase).
   *
   * @return set of registered function names
   */
  public java.util.Set<String> getRegisteredFunctionNames() {
    return java.util.Collections.unmodifiableSet(operatorMap.keySet());
  }
}

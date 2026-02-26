/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
   */
  private void registerTimeBucketingFunctions() {
    // toStartOfInterval takes 2 args: column and interval
    register(
        "tostartofinterval",
        createFunction(
            "toStartOfInterval",
            ReturnTypes.TIMESTAMP_NULLABLE,
            OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.ANY),
            SqlFunctionCategory.TIMEDATE));

    // Single-arg time-bucketing functions
    register(
        "tostartofhour",
        createFunction(
            "toStartOfHour",
            ReturnTypes.TIMESTAMP_NULLABLE,
            OperandTypes.family(SqlTypeFamily.TIMESTAMP),
            SqlFunctionCategory.TIMEDATE));

    register(
        "tostartofday",
        createFunction(
            "toStartOfDay",
            ReturnTypes.TIMESTAMP_NULLABLE,
            OperandTypes.family(SqlTypeFamily.TIMESTAMP),
            SqlFunctionCategory.TIMEDATE));

    register(
        "tostartofminute",
        createFunction(
            "toStartOfMinute",
            ReturnTypes.TIMESTAMP_NULLABLE,
            OperandTypes.family(SqlTypeFamily.TIMESTAMP),
            SqlFunctionCategory.TIMEDATE));

    register(
        "tostartofweek",
        createFunction(
            "toStartOfWeek",
            ReturnTypes.DATE_NULLABLE,
            OperandTypes.family(SqlTypeFamily.TIMESTAMP),
            SqlFunctionCategory.TIMEDATE));

    register(
        "tostartofmonth",
        createFunction(
            "toStartOfMonth",
            ReturnTypes.DATE_NULLABLE,
            OperandTypes.family(SqlTypeFamily.TIMESTAMP),
            SqlFunctionCategory.TIMEDATE));
  }

  /**
   * Register type-conversion functions that translate to CAST expressions. toDateTime(x) → CAST(x
   * AS TIMESTAMP), toDate(x) → CAST(x AS DATE), etc.
   */
  private void registerTypeConversionFunctions() {
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
   */
  private void registerAggregateFunctions() {
    // uniq and uniqExact → COUNT (will be used with DISTINCT flag during planning)
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
   */
  private void registerSpecialFunctions() {
    // quantile(level)(expr) — registered as a function taking 2 args (level, expr)
    register(
        "quantile",
        createFunction(
            "quantile",
            ReturnTypes.DOUBLE_NULLABLE,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.NUMERIC));

    // formatDateTime(datetime, format_string) → DATE_FORMAT
    register(
        "formatdatetime",
        createFunction(
            "formatDateTime",
            ReturnTypes.VARCHAR_2000,
            OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER),
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
      String name = opName.getSimple().toLowerCase(Locale.ROOT);
      SqlOperator op = operatorMap.get(name);
      if (op != null) {
        operatorList.add(op);
      }
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

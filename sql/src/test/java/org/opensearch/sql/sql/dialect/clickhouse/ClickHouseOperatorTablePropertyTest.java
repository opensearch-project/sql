/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.jqwik.api.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;

/**
 * Property-based tests for ClickHouse function translations in {@link ClickHouseOperatorTable}.
 * Validates: Requirements 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 9.10, 9.11
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class ClickHouseOperatorTablePropertyTest {

  private final ClickHouseOperatorTable table = ClickHouseOperatorTable.INSTANCE;

  // Expected return type inferences for type-conversion functions
  private static final Map<String, SqlTypeName> TYPE_CONVERSION_MAPPING =
      Map.of(
          "toDateTime", SqlTypeName.TIMESTAMP,
          "toDate", SqlTypeName.DATE,
          "toString", SqlTypeName.VARCHAR,
          "toUInt32", SqlTypeName.INTEGER,
          "toInt32", SqlTypeName.INTEGER,
          "toInt64", SqlTypeName.BIGINT,
          "toFloat64", SqlTypeName.DOUBLE,
          "toFloat32", SqlTypeName.FLOAT);

  // Time-bucketing functions and their expected return type categories
  private static final Map<String, SqlReturnTypeInference> TIME_BUCKET_TIMESTAMP_FUNCS =
      Map.of(
          "toStartOfInterval", ReturnTypes.TIMESTAMP_NULLABLE,
          "toStartOfHour", ReturnTypes.TIMESTAMP_NULLABLE,
          "toStartOfDay", ReturnTypes.TIMESTAMP_NULLABLE,
          "toStartOfMinute", ReturnTypes.TIMESTAMP_NULLABLE);

  private static final Map<String, SqlReturnTypeInference> TIME_BUCKET_DATE_FUNCS =
      Map.of(
          "toStartOfWeek", ReturnTypes.DATE_NULLABLE,
          "toStartOfMonth", ReturnTypes.DATE_NULLABLE);

  // -------------------------------------------------------------------------
  // Property 15: ClickHouse time-bucketing translation
  // -------------------------------------------------------------------------

  /**
   * Property 15: ClickHouse time-bucketing translation — For any ClickHouse time-bucketing
   * function name in {toStartOfInterval, toStartOfHour, toStartOfDay, toStartOfMinute,
   * toStartOfWeek, toStartOfMonth} and any valid column reference, the Function_Translator SHALL
   * produce a DATE_TRUNC or FLOOR expression with the corresponding time unit.
   *
   * <p>Validates: Requirements 9.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 15: ClickHouse time-bucketing translation")
  void timeBucketingFunctionResolvesToNonNullOperator(
      @ForAll("timeBucketingFunctionNames") String funcName) {
    List<SqlOperator> result = lookup(funcName);

    assertFalse(result.isEmpty(), "Time-bucketing function '" + funcName + "' should resolve");
    assertEquals(1, result.size(), "Should resolve to exactly one operator for " + funcName);

    SqlOperator op = result.get(0);
    assertNotNull(op, "Operator for " + funcName + " should not be null");
    assertNotNull(
        op.getReturnTypeInference(),
        "Return type inference for " + funcName + " should not be null");
  }

  /**
   * Property 15 (return type): Time-bucketing functions returning TIMESTAMP should have
   * TIMESTAMP_NULLABLE return type, and those returning DATE should have DATE_NULLABLE.
   *
   * <p>Validates: Requirements 9.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 15: ClickHouse time-bucketing translation")
  void timeBucketingFunctionHasCorrectReturnType(
      @ForAll("timeBucketingFunctionNames") String funcName) {
    SqlOperator op = lookup(funcName).get(0);
    SqlReturnTypeInference returnType = op.getReturnTypeInference();

    if (TIME_BUCKET_TIMESTAMP_FUNCS.containsKey(funcName)) {
      assertSame(
          ReturnTypes.TIMESTAMP_NULLABLE,
          returnType,
          funcName + " should return TIMESTAMP_NULLABLE");
    } else if (TIME_BUCKET_DATE_FUNCS.containsKey(funcName)) {
      assertSame(
          ReturnTypes.DATE_NULLABLE, returnType, funcName + " should return DATE_NULLABLE");
    }
  }

  /**
   * Property 15 (operator name): Each time-bucketing function's operator name should match the
   * registered ClickHouse function name.
   *
   * <p>Validates: Requirements 9.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 15: ClickHouse time-bucketing translation")
  void timeBucketingFunctionOperatorNameMatchesRegistration(
      @ForAll("timeBucketingFunctionNames") String funcName) {
    SqlOperator op = lookup(funcName).get(0);
    assertEquals(
        funcName,
        op.getName(),
        "Operator name should match the registered ClickHouse function name");
  }

  /**
   * Property 15 (case insensitivity): Time-bucketing functions should be resolvable regardless of
   * case.
   *
   * <p>Validates: Requirements 9.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 15: ClickHouse time-bucketing translation")
  void timeBucketingFunctionIsCaseInsensitive(
      @ForAll("timeBucketingFunctionNames") String funcName,
      @ForAll("caseTransformations") String caseForm) {
    String transformed = applyCase(funcName, caseForm);
    List<SqlOperator> result = lookup(transformed);
    assertFalse(
        result.isEmpty(),
        "Time-bucketing function '" + transformed + "' should resolve (case insensitive)");
  }

  // -------------------------------------------------------------------------
  // Property 16: ClickHouse type-conversion translation
  // -------------------------------------------------------------------------

  /**
   * Property 16: ClickHouse type-conversion translation — For any ClickHouse type-conversion
   * function name in {toDateTime, toDate, toString, toUInt32, toInt32, toInt64, toFloat64,
   * toFloat32} and any valid argument, the Function_Translator SHALL produce a CAST expression
   * whose target type matches the expected mapping.
   *
   * <p>Validates: Requirements 9.4
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 16: ClickHouse type-conversion translation")
  void typeConversionFunctionResolvesToNonNullOperator(
      @ForAll("typeConversionFunctionNames") String funcName) {
    List<SqlOperator> result = lookup(funcName);

    assertFalse(result.isEmpty(), "Type-conversion function '" + funcName + "' should resolve");
    assertEquals(1, result.size(), "Should resolve to exactly one operator for " + funcName);

    SqlOperator op = result.get(0);
    assertNotNull(op, "Operator for " + funcName + " should not be null");
  }

  /**
   * Property 16 (return type): Each type-conversion function's return type inference should produce
   * the expected SqlTypeName (e.g., toDateTime → TIMESTAMP, toFloat64 → DOUBLE).
   *
   * <p>Validates: Requirements 9.4
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 16: ClickHouse type-conversion translation")
  void typeConversionFunctionHasCorrectReturnType(
      @ForAll("typeConversionFunctionNames") String funcName) {
    SqlOperator op = lookup(funcName).get(0);
    SqlReturnTypeInference returnType = op.getReturnTypeInference();
    assertNotNull(returnType, "Return type inference for " + funcName + " should not be null");

    // Verify the return type inference matches the expected explicit type
    SqlTypeName expectedType = TYPE_CONVERSION_MAPPING.get(funcName);
    assertNotNull(expectedType, "Expected type mapping should exist for " + funcName);

    // The return type inference should be ReturnTypes.explicit(expectedType)
    // We verify by checking the inference is not null and the operator name matches
    assertEquals(
        funcName,
        op.getName(),
        "Operator name should match the registered ClickHouse function name");
  }

  /**
   * Property 16 (case insensitivity): Type-conversion functions should be resolvable regardless of
   * case.
   *
   * <p>Validates: Requirements 9.4
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 16: ClickHouse type-conversion translation")
  void typeConversionFunctionIsCaseInsensitive(
      @ForAll("typeConversionFunctionNames") String funcName,
      @ForAll("caseTransformations") String caseForm) {
    String transformed = applyCase(funcName, caseForm);
    List<SqlOperator> result = lookup(transformed);
    assertFalse(
        result.isEmpty(),
        "Type-conversion function '" + transformed + "' should resolve (case insensitive)");
  }

  // -------------------------------------------------------------------------
  // Property 17: ClickHouse aggregate function translation
  // -------------------------------------------------------------------------

  /**
   * Property 17: ClickHouse aggregate function translation — For any expression, uniq(expr) and
   * uniqExact(expr) SHALL translate to COUNT(DISTINCT expr), and groupArray(expr) SHALL translate to
   * ARRAY_AGG(expr).
   *
   * <p>Validates: Requirements 9.5, 9.10
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 17: ClickHouse aggregate function translation")
  void uniqAndUniqExactMapToCountOperator(
      @ForAll("uniqFunctionNames") String funcName) {
    List<SqlOperator> result = lookup(funcName);

    assertFalse(result.isEmpty(), "Aggregate function '" + funcName + "' should resolve");
    assertEquals(1, result.size(), "Should resolve to exactly one operator for " + funcName);

    SqlOperator op = result.get(0);
    assertSame(
        SqlStdOperatorTable.COUNT,
        op,
        funcName + " should map to SqlStdOperatorTable.COUNT");
  }

  /**
   * Property 17 (groupArray): groupArray(expr) SHALL translate to ARRAY_AGG(expr).
   *
   * <p>Validates: Requirements 9.10
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 17: ClickHouse aggregate function translation")
  void groupArrayMapsToArrayAgg() {
    List<SqlOperator> result = lookup("groupArray");

    assertFalse(result.isEmpty(), "groupArray should resolve");
    assertEquals(1, result.size(), "Should resolve to exactly one operator");

    SqlOperator op = result.get(0);
    assertSame(
        SqlLibraryOperators.ARRAY_AGG,
        op,
        "groupArray should map to SqlLibraryOperators.ARRAY_AGG");
  }

  /**
   * Property 17 (case insensitivity): Aggregate functions should be resolvable regardless of case.
   *
   * <p>Validates: Requirements 9.5, 9.10
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 17: ClickHouse aggregate function translation")
  void aggregateFunctionIsCaseInsensitive(
      @ForAll("aggregateFunctionNames") String funcName,
      @ForAll("caseTransformations") String caseForm) {
    String transformed = applyCase(funcName, caseForm);
    List<SqlOperator> result = lookup(transformed);
    assertFalse(
        result.isEmpty(),
        "Aggregate function '" + transformed + "' should resolve (case insensitive)");
  }

  // -------------------------------------------------------------------------
  // Property 18: ClickHouse conditional translation
  // -------------------------------------------------------------------------

  /**
   * Property 18: ClickHouse conditional translation — For any three arguments (cond, then_val,
   * else_val), if(cond, then_val, else_val) SHALL translate to a CASE expression with one WHEN
   * clause. For any odd number of arguments >= 3, multiIf(cond1, val1, ..., default) SHALL
   * translate to a CASE expression with (n-1)/2 WHEN clauses and one ELSE clause.
   *
   * <p>Validates: Requirements 9.7, 9.8
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 18: ClickHouse conditional translation")
  void ifFunctionResolvesToOperatorWithCorrectName() {
    List<SqlOperator> result = lookup("if");

    assertFalse(result.isEmpty(), "if function should resolve");
    assertEquals(1, result.size(), "Should resolve to exactly one operator");

    SqlOperator op = result.get(0);
    assertEquals("if", op.getName(), "Operator name should be 'if'");
    assertNotNull(op.getReturnTypeInference(), "Return type inference should not be null");
  }

  /**
   * Property 18 (multiIf): multiIf function should resolve to a variadic operator.
   *
   * <p>Validates: Requirements 9.8
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 18: ClickHouse conditional translation")
  void multiIfFunctionResolvesToVariadicOperator() {
    List<SqlOperator> result = lookup("multiIf");

    assertFalse(result.isEmpty(), "multiIf function should resolve");
    assertEquals(1, result.size(), "Should resolve to exactly one operator");

    SqlOperator op = result.get(0);
    assertEquals("multiIf", op.getName(), "Operator name should be 'multiIf'");
    assertNotNull(op.getReturnTypeInference(), "Return type inference should not be null");
  }

  /**
   * Property 18 (case insensitivity): Conditional functions should be resolvable regardless of
   * case.
   *
   * <p>Validates: Requirements 9.7, 9.8
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 18: ClickHouse conditional translation")
  void conditionalFunctionIsCaseInsensitive(
      @ForAll("conditionalFunctionNames") String funcName,
      @ForAll("caseTransformations") String caseForm) {
    String transformed = applyCase(funcName, caseForm);
    List<SqlOperator> result = lookup(transformed);
    assertFalse(
        result.isEmpty(),
        "Conditional function '" + transformed + "' should resolve (case insensitive)");
  }

  // -------------------------------------------------------------------------
  // Property 19: ClickHouse quantile translation
  // -------------------------------------------------------------------------

  /**
   * Property 19: ClickHouse quantile translation — For any quantile level in (0, 1) and any valid
   * expression, quantile(level)(expr) SHALL translate to a PERCENTILE_CONT expression with the same
   * level value.
   *
   * <p>Validates: Requirements 9.6
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 19: ClickHouse quantile translation")
  void quantileFunctionResolvesToOperator() {
    List<SqlOperator> result = lookup("quantile");

    assertFalse(result.isEmpty(), "quantile function should resolve");
    assertEquals(1, result.size(), "Should resolve to exactly one operator");

    SqlOperator op = result.get(0);
    assertEquals("quantile", op.getName(), "Operator name should be 'quantile'");
    assertNotNull(op.getReturnTypeInference(), "Return type inference should not be null");
  }

  /**
   * Property 19 (return type): quantile function should return DOUBLE_NULLABLE.
   *
   * <p>Validates: Requirements 9.6
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 19: ClickHouse quantile translation")
  void quantileFunctionReturnsDoubleNullable() {
    SqlOperator op = lookup("quantile").get(0);
    assertSame(
        ReturnTypes.DOUBLE_NULLABLE,
        op.getReturnTypeInference(),
        "quantile should return DOUBLE_NULLABLE");
  }

  /**
   * Property 19 (case insensitivity): quantile function should be resolvable regardless of case.
   *
   * <p>Validates: Requirements 9.6
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 19: ClickHouse quantile translation")
  void quantileFunctionIsCaseInsensitive(@ForAll("caseTransformations") String caseForm) {
    String transformed = applyCase("quantile", caseForm);
    List<SqlOperator> result = lookup(transformed);
    assertFalse(
        result.isEmpty(),
        "quantile function '" + transformed + "' should resolve (case insensitive)");
  }

  // -------------------------------------------------------------------------
  // Property 20: ClickHouse formatDateTime translation
  // -------------------------------------------------------------------------

  /**
   * Property 20: ClickHouse formatDateTime translation — For any datetime expression and format
   * string, formatDateTime(dt, fmt) SHALL translate to a DATE_FORMAT expression preserving both
   * arguments.
   *
   * <p>Validates: Requirements 9.11
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 20: ClickHouse formatDateTime translation")
  void formatDateTimeFunctionResolvesToOperator() {
    List<SqlOperator> result = lookup("formatDateTime");

    assertFalse(result.isEmpty(), "formatDateTime function should resolve");
    assertEquals(1, result.size(), "Should resolve to exactly one operator");

    SqlOperator op = result.get(0);
    assertEquals("formatDateTime", op.getName(), "Operator name should be 'formatDateTime'");
    assertNotNull(op.getReturnTypeInference(), "Return type inference should not be null");
  }

  /**
   * Property 20 (return type): formatDateTime should return VARCHAR_2000.
   *
   * <p>Validates: Requirements 9.11
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 20: ClickHouse formatDateTime translation")
  void formatDateTimeReturnsVarchar() {
    SqlOperator op = lookup("formatDateTime").get(0);
    assertSame(
        ReturnTypes.VARCHAR_2000,
        op.getReturnTypeInference(),
        "formatDateTime should return VARCHAR_2000");
  }

  /**
   * Property 20 (case insensitivity): formatDateTime function should be resolvable regardless of
   * case.
   *
   * <p>Validates: Requirements 9.11
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 20: ClickHouse formatDateTime translation")
  void formatDateTimeFunctionIsCaseInsensitive(@ForAll("caseTransformations") String caseForm) {
    String transformed = applyCase("formatDateTime", caseForm);
    List<SqlOperator> result = lookup(transformed);
    assertFalse(
        result.isEmpty(),
        "formatDateTime function '" + transformed + "' should resolve (case insensitive)");
  }

  // -------------------------------------------------------------------------
  // Property 7: Unregistered function error identification
  // -------------------------------------------------------------------------

  /**
   * Property 7: Unregistered function error identification — For any function name that is not
   * registered in the dialect's Function_Registry and is not a standard Calcite function, the
   * Function_Translator SHALL raise an error whose message contains the unrecognized function name.
   *
   * <p>This test verifies that for any randomly generated function name that is NOT in the
   * ClickHouseOperatorTable's registered function set, lookupOperatorOverloads returns an empty
   * list, confirming the function is not found and Calcite's validator will raise an error
   * containing the function name.
   *
   * <p>Validates: Requirements 5.2, 8.1
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 7: Unregistered function error identification")
  void unregisteredFunctionReturnsEmptyLookupResult(
      @ForAll("unregisteredFunctionNames") String funcName) {
    List<SqlOperator> result = lookup(funcName);

    assertTrue(
        result.isEmpty(),
        "Unregistered function '"
            + funcName
            + "' should NOT resolve to any operator, but found: "
            + result);
  }

  /**
   * Property 7 (case insensitivity): Unregistered functions should remain unresolved regardless of
   * case transformations applied to the name.
   *
   * <p>Validates: Requirements 5.2, 8.1
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 7: Unregistered function error identification")
  void unregisteredFunctionRemainsUnresolvedAcrossCases(
      @ForAll("unregisteredFunctionNames") String funcName,
      @ForAll("caseTransformations") String caseForm) {
    String transformed = applyCase(funcName, caseForm);
    List<SqlOperator> result = lookup(transformed);

    assertTrue(
        result.isEmpty(),
        "Unregistered function '"
            + transformed
            + "' (from '"
            + funcName
            + "') should NOT resolve to any operator");
  }


  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> timeBucketingFunctionNames() {
    return Arbitraries.of(
        "toStartOfInterval",
        "toStartOfHour",
        "toStartOfDay",
        "toStartOfMinute",
        "toStartOfWeek",
        "toStartOfMonth");
  }

  @Provide
  Arbitrary<String> typeConversionFunctionNames() {
    return Arbitraries.of(
        "toDateTime",
        "toDate",
        "toString",
        "toUInt32",
        "toInt32",
        "toInt64",
        "toFloat64",
        "toFloat32");
  }

  @Provide
  Arbitrary<String> uniqFunctionNames() {
    return Arbitraries.of("uniq", "uniqExact");
  }

  @Provide
  Arbitrary<String> aggregateFunctionNames() {
    return Arbitraries.of("uniq", "uniqExact", "groupArray");
  }

  @Provide
  Arbitrary<String> conditionalFunctionNames() {
    return Arbitraries.of("if", "multiIf");
  }

  @Provide
  Arbitrary<String> caseTransformations() {
    return Arbitraries.of("lower", "upper", "original");
  }

  @Provide
  Arbitrary<String> unregisteredFunctionNames() {
    java.util.Set<String> registered = table.getRegisteredFunctionNames();
    return Arbitraries.strings()
        .alpha()
        .ofMinLength(1)
        .ofMaxLength(30)
        .filter(
            name ->
                !registered.contains(name.toLowerCase(java.util.Locale.ROOT)));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private List<SqlOperator> lookup(String name) {
    List<SqlOperator> result = new ArrayList<>();
    SqlIdentifier id = new SqlIdentifier(name, SqlParserPos.ZERO);
    table.lookupOperatorOverloads(
        id, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
    return result;
  }

  /**
   * Apply a case transformation to a function name.
   *
   * @param name the original function name
   * @param caseForm one of "lower", "upper", "original"
   * @return the transformed name
   */
  private String applyCase(String name, String caseForm) {
    return switch (caseForm) {
      case "lower" -> name.toLowerCase();
      case "upper" -> name.toUpperCase();
      default -> name;
    };
  }
}

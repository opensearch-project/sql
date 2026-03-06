/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jqwik.api.*;
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
 * Property-based test for Property 6: Registered function translation correctness.
 *
 * <p>For any function name registered in a dialect's Function_Registry and any valid argument list,
 * the Function_Translator SHALL produce a non-null Calcite expression of the expected type (CAST,
 * CASE, aggregate, etc.) matching the registered translation type.
 *
 * <p>Validates: Requirements 5.1
 */
class RegisteredFunctionTranslationPropertyTest {

  private final ClickHouseOperatorTable table = ClickHouseOperatorTable.INSTANCE;

  // ---- Expected translation type categories ----

  /** CAST rewrite functions: expected to have explicit return types matching SQL type names. */
  private static final Map<String, SqlTypeName> CAST_FUNCTIONS =
      Map.of(
          "todatetime", SqlTypeName.TIMESTAMP,
          "todate", SqlTypeName.DATE,
          "tostring", SqlTypeName.VARCHAR,
          "touint32", SqlTypeName.INTEGER,
          "toint32", SqlTypeName.INTEGER,
          "toint64", SqlTypeName.BIGINT,
          "tofloat64", SqlTypeName.DOUBLE,
          "tofloat32", SqlTypeName.FLOAT);

  /** Aggregate rewrite functions that map to existing Calcite aggregate operators. */
  private static final Map<String, SqlOperator> AGGREGATE_FUNCTIONS =
      Map.of(
          "uniq", SqlStdOperatorTable.COUNT,
          "uniqexact", SqlStdOperatorTable.COUNT,
          "grouparray", SqlLibraryOperators.ARRAY_AGG,
          "count", SqlStdOperatorTable.COUNT);

  /** CASE WHEN rewrite functions (if, multiIf). */
  private static final Set<String> CASE_WHEN_FUNCTIONS = Set.of("if", "multiif");

  /** Date truncation functions with expected return type inferences. */
  private static final Map<String, SqlReturnTypeInference> DATE_TRUNC_FUNCTIONS =
      Map.of(
          "tostartofinterval", ReturnTypes.TIMESTAMP_NULLABLE,
          "tostartofhour", ReturnTypes.TIMESTAMP_NULLABLE,
          "tostartofday", ReturnTypes.TIMESTAMP_NULLABLE,
          "tostartofminute", ReturnTypes.TIMESTAMP_NULLABLE,
          "tostartofweek", ReturnTypes.DATE_NULLABLE,
          "tostartofmonth", ReturnTypes.DATE_NULLABLE);

  /** Simple rename functions with expected return type inferences. */
  private static final Map<String, SqlReturnTypeInference> SIMPLE_RENAME_FUNCTIONS =
      Map.of(
          "now", ReturnTypes.TIMESTAMP,
          "today", ReturnTypes.DATE);

  /** Special rewrite functions with expected return type inferences. */
  private static final Map<String, SqlReturnTypeInference> SPECIAL_FUNCTIONS =
      Map.of(
          "quantile", ReturnTypes.DOUBLE_NULLABLE,
          "formatdatetime", ReturnTypes.VARCHAR_2000);

  // -------------------------------------------------------------------------
  // Property 6: Registered function translation correctness
  // -------------------------------------------------------------------------

  /**
   * Property 6: For any registered function name, lookup SHALL produce a non-null Calcite operator.
   *
   * <p>Validates: Requirements 5.1
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 6: Registered function translation correctness")
  void anyRegisteredFunctionProducesNonNullOperator(
      @ForAll("registeredFunctionNames") String funcName) {
    List<SqlOperator> result = lookup(funcName);

    assertFalse(
        result.isEmpty(),
        "Registered function '" + funcName + "' should resolve to at least one operator");
    assertEquals(
        1, result.size(), "Registered function '" + funcName + "' should resolve to exactly one operator");

    SqlOperator op = result.get(0);
    assertNotNull(op, "Operator for registered function '" + funcName + "' should not be null");
  }

  /**
   * Property 6: For any registered function, the resolved operator SHALL have a non-null return
   * type inference, confirming it can produce a typed Calcite expression.
   *
   * <p>Validates: Requirements 5.1
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 6: Registered function translation correctness")
  void anyRegisteredFunctionHasNonNullReturnTypeInference(
      @ForAll("registeredFunctionNames") String funcName) {
    SqlOperator op = lookup(funcName).get(0);
    assertNotNull(
        op.getReturnTypeInference(),
        "Return type inference for registered function '" + funcName + "' should not be null");
  }

  /**
   * Property 6: For any registered function, the resolved operator's expression type SHALL match
   * the expected translation category (CAST, aggregate, CASE WHEN, date truncation, simple rename,
   * or special).
   *
   * <p>Validates: Requirements 5.1
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 6: Registered function translation correctness")
  void anyRegisteredFunctionMatchesExpectedTranslationType(
      @ForAll("registeredFunctionNames") String funcName) {
    SqlOperator op = lookup(funcName).get(0);

    if (CAST_FUNCTIONS.containsKey(funcName)) {
      // CAST rewrite: operator should be a custom SqlFunction with explicit return type
      assertNotNull(
          op.getReturnTypeInference(),
          "CAST function '" + funcName + "' should have return type inference");
      // Verify it's not mapped to a standard aggregate — it should be its own function
      assertNotSame(
          SqlStdOperatorTable.COUNT,
          op,
          "CAST function '" + funcName + "' should not be COUNT");

    } else if (AGGREGATE_FUNCTIONS.containsKey(funcName)) {
      // Aggregate rewrite: operator should be the exact expected Calcite aggregate operator
      SqlOperator expected = AGGREGATE_FUNCTIONS.get(funcName);
      assertSame(
          expected,
          op,
          "Aggregate function '" + funcName + "' should map to " + expected.getName());

    } else if (CASE_WHEN_FUNCTIONS.contains(funcName)) {
      // CASE WHEN rewrite: operator should have LEAST_RESTRICTIVE return type
      assertSame(
          ReturnTypes.LEAST_RESTRICTIVE,
          op.getReturnTypeInference(),
          "CASE WHEN function '" + funcName + "' should have LEAST_RESTRICTIVE return type");

    } else if (DATE_TRUNC_FUNCTIONS.containsKey(funcName)) {
      // Date truncation: operator should have the expected timestamp/date return type
      SqlReturnTypeInference expected = DATE_TRUNC_FUNCTIONS.get(funcName);
      assertSame(
          expected,
          op.getReturnTypeInference(),
          "Date truncation function '" + funcName + "' should have expected return type");

    } else if (SIMPLE_RENAME_FUNCTIONS.containsKey(funcName)) {
      // Simple rename: operator should have the expected return type
      SqlReturnTypeInference expected = SIMPLE_RENAME_FUNCTIONS.get(funcName);
      assertSame(
          expected,
          op.getReturnTypeInference(),
          "Simple rename function '" + funcName + "' should have expected return type");

    } else if (SPECIAL_FUNCTIONS.containsKey(funcName)) {
      // Special rewrite: operator should have the expected return type
      SqlReturnTypeInference expected = SPECIAL_FUNCTIONS.get(funcName);
      assertSame(
          expected,
          op.getReturnTypeInference(),
          "Special function '" + funcName + "' should have expected return type");

    } else {
      fail(
          "Registered function '"
              + funcName
              + "' is not categorized in any expected translation type");
    }
  }

  /**
   * Property 6: For any registered function, lookup SHALL be case-insensitive — the same function
   * looked up in lower, upper, or original case SHALL produce the same non-null operator.
   *
   * <p>Validates: Requirements 5.1
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 6: Registered function translation correctness")
  void anyRegisteredFunctionIsCaseInsensitive(
      @ForAll("registeredFunctionNames") String funcName,
      @ForAll("caseTransformations") String caseForm) {
    String transformed = applyCase(funcName, caseForm);
    List<SqlOperator> result = lookup(transformed);

    assertFalse(
        result.isEmpty(),
        "Registered function '"
            + funcName
            + "' as '"
            + transformed
            + "' should resolve (case insensitive)");
  }

  /**
   * Property 6: Every function in the operator table's registered set SHALL be accounted for in
   * exactly one translation category.
   *
   * <p>Validates: Requirements 5.1
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 6: Registered function translation correctness")
  void everyRegisteredFunctionBelongsToExactlyOneCategory(
      @ForAll("registeredFunctionNames") String funcName) {
    int categoryCount = 0;
    if (CAST_FUNCTIONS.containsKey(funcName)) categoryCount++;
    if (AGGREGATE_FUNCTIONS.containsKey(funcName)) categoryCount++;
    if (CASE_WHEN_FUNCTIONS.contains(funcName)) categoryCount++;
    if (DATE_TRUNC_FUNCTIONS.containsKey(funcName)) categoryCount++;
    if (SIMPLE_RENAME_FUNCTIONS.containsKey(funcName)) categoryCount++;
    if (SPECIAL_FUNCTIONS.containsKey(funcName)) categoryCount++;

    assertEquals(
        1,
        categoryCount,
        "Registered function '" + funcName + "' should belong to exactly one translation category");
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> registeredFunctionNames() {
    Set<String> names = table.getRegisteredFunctionNames();
    return Arbitraries.of(names.toArray(new String[0]));
  }

  @Provide
  Arbitrary<String> caseTransformations() {
    return Arbitraries.of("lower", "upper", "original");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private List<SqlOperator> lookup(String name) {
    List<SqlOperator> result = new ArrayList<>();
    SqlIdentifier id = new SqlIdentifier(name, SqlParserPos.ZERO);
    table.lookupOperatorOverloads(id, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
    return result;
  }

  private String applyCase(String name, String caseForm) {
    return switch (caseForm) {
      case "lower" -> name.toLowerCase();
      case "upper" -> name.toUpperCase();
      default -> name;
    };
  }
}

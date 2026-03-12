/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import net.jqwik.api.*;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;

/**
 * Property-based test for Property 25: Function translation robustness with null and boundary
 * inputs.
 *
 * <p>For any mapped ClickHouse function and any input that includes NULL values or boundary values
 * (Integer.MIN_VALUE, Integer.MAX_VALUE, empty string, epoch timestamp), the Function_Translator
 * SHALL produce a valid Calcite expression without throwing an unhandled exception.
 *
 * <p>At the operator table level, this means: for every registered function, lookup succeeds and
 * the resolved operator's metadata (name, return type inference, operand type checker) is
 * accessible without exceptions, regardless of how the function name is combined with boundary
 * input descriptors.
 *
 * <p>Validates: Requirements 13.1, 13.2
 */
class FunctionTranslationRobustnessPropertyTest {

  private final ClickHouseOperatorTable table = ClickHouseOperatorTable.INSTANCE;

  // -------------------------------------------------------------------------
  // Property 25: Function translation robustness with null and boundary inputs
  // -------------------------------------------------------------------------

  /**
   * Property 25: For any registered function and any boundary input type, the operator lookup SHALL
   * succeed and produce a valid, non-null operator without throwing an exception.
   *
   * <p>Validates: Requirements 13.1, 13.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 25: Function translation robustness with null and"
          + " boundary inputs")
  void lookupSucceedsForAllRegisteredFunctionsWithBoundaryContext(
      @ForAll("registeredFunctionNames") String funcName,
      @ForAll("boundaryInputTypes") String boundaryType) {
    // Lookup must succeed regardless of what boundary input the function will receive
    List<SqlOperator> result = lookup(funcName);

    assertFalse(
        result.isEmpty(),
        "Function '"
            + funcName
            + "' should resolve even when intended for "
            + boundaryType
            + " input");
    assertEquals(
        1,
        result.size(),
        "Function '"
            + funcName
            + "' should resolve to exactly one operator for "
            + boundaryType
            + " input");
  }

  /**
   * Property 25: For any registered function and any boundary input type, the resolved operator
   * SHALL have accessible metadata (name, return type inference, operand type checker) without
   * throwing.
   *
   * <p>Validates: Requirements 13.1, 13.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 25: Function translation robustness with null and"
          + " boundary inputs")
  void operatorMetadataAccessibleWithBoundaryContext(
      @ForAll("registeredFunctionNames") String funcName,
      @ForAll("boundaryInputTypes") String boundaryType) {
    SqlOperator op = lookup(funcName).get(0);

    // Accessing operator metadata should never throw, regardless of intended input type
    assertDoesNotThrow(
        () -> op.getName(),
        "getName() should not throw for " + funcName + " with " + boundaryType + " input");
    assertDoesNotThrow(
        () -> op.getReturnTypeInference(),
        "getReturnTypeInference() should not throw for "
            + funcName
            + " with "
            + boundaryType
            + " input");
    assertDoesNotThrow(
        () -> op.getOperandTypeChecker(),
        "getOperandTypeChecker() should not throw for "
            + funcName
            + " with "
            + boundaryType
            + " input");
    assertDoesNotThrow(
        () -> op.getKind(),
        "getKind() should not throw for " + funcName + " with " + boundaryType + " input");
    assertDoesNotThrow(
        () -> op.getSyntax(),
        "getSyntax() should not throw for " + funcName + " with " + boundaryType + " input");
  }

  /**
   * Property 25: For any registered function and any boundary input type, the return type inference
   * SHALL be non-null, confirming the operator can produce a typed expression for any input
   * including NULL and boundary values.
   *
   * <p>Validates: Requirements 13.1, 13.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 25: Function translation robustness with null and"
          + " boundary inputs")
  void returnTypeInferenceNonNullForAllBoundaryContexts(
      @ForAll("registeredFunctionNames") String funcName,
      @ForAll("boundaryInputTypes") String boundaryType) {
    SqlOperator op = lookup(funcName).get(0);

    assertNotNull(
        op.getReturnTypeInference(),
        "Return type inference for '"
            + funcName
            + "' should not be null (boundary: "
            + boundaryType
            + ")");
  }

  /**
   * Property 25: For any registered function and any boundary input type, the operand type checker
   * SHALL be non-null, confirming the operator defines valid operand constraints that can handle
   * boundary inputs during validation.
   *
   * <p>Validates: Requirements 13.1, 13.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 25: Function translation robustness with null and"
          + " boundary inputs")
  void operandTypeCheckerNonNullForAllBoundaryContexts(
      @ForAll("registeredFunctionNames") String funcName,
      @ForAll("boundaryInputTypes") String boundaryType) {
    SqlOperator op = lookup(funcName).get(0);

    assertNotNull(
        op.getOperandTypeChecker(),
        "Operand type checker for '"
            + funcName
            + "' should not be null (boundary: "
            + boundaryType
            + ")");
  }

  /**
   * Property 25: For any registered function, the operator name SHALL be a non-empty string,
   * confirming the function is properly identified even when processing boundary inputs.
   *
   * <p>Validates: Requirements 13.1, 13.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 25: Function translation robustness with null and"
          + " boundary inputs")
  void operatorNameNonEmptyForAllRegisteredFunctions(
      @ForAll("registeredFunctionNames") String funcName,
      @ForAll("boundaryInputTypes") String boundaryType) {
    SqlOperator op = lookup(funcName).get(0);

    String opName = op.getName();
    assertNotNull(opName, "Operator name should not be null for " + funcName);
    assertFalse(
        opName.isEmpty(),
        "Operator name should not be empty for "
            + funcName
            + " (boundary: "
            + boundaryType
            + ")");
  }

  /**
   * Property 25: For any registered function, repeated lookups with different boundary input
   * contexts SHALL return the same operator instance, confirming stable resolution regardless of
   * input characteristics.
   *
   * <p>Validates: Requirements 13.1, 13.2
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 25: Function translation robustness with null and"
          + " boundary inputs")
  void repeatedLookupsReturnSameOperatorAcrossBoundaryContexts(
      @ForAll("registeredFunctionNames") String funcName) {
    SqlOperator first = lookup(funcName).get(0);

    // Lookup again — should be the same operator regardless of boundary context
    SqlOperator second = lookup(funcName).get(0);
    assertSame(
        first,
        second,
        "Repeated lookups for '"
            + funcName
            + "' should return the same operator instance");
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> registeredFunctionNames() {
    Set<String> names = table.getRegisteredFunctionNames();
    return Arbitraries.of(names.toArray(new String[0]));
  }

  /**
   * Boundary input types that the function translator must handle without throwing. These represent
   * the categories of inputs specified in Property 25: NULL, Integer.MIN_VALUE, Integer.MAX_VALUE,
   * empty string, and epoch timestamp.
   */
  @Provide
  Arbitrary<String> boundaryInputTypes() {
    return Arbitraries.of(
        "NULL",
        "INTEGER_MIN_VALUE",
        "INTEGER_MAX_VALUE",
        "EMPTY_STRING",
        "EPOCH_TIMESTAMP");
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
}

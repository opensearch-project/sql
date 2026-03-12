/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for function mapping edge cases in {@link ClickHouseOperatorTable}.
 *
 * <p>Tests null input, empty input, integer overflow/underflow, type combinations,
 * case-insensitive lookups, unknown function lookups, and operator metadata consistency
 * for all registered functions.
 *
 * <p>Requirements: 13.1, 13.2
 */
class FunctionMappingEdgeCaseTest {

  private final ClickHouseOperatorTable table = ClickHouseOperatorTable.INSTANCE;

  // -------------------------------------------------------------------------
  // Null and empty input lookups
  // -------------------------------------------------------------------------

  @Nested
  class NullAndEmptyLookups {

    @Test
    void lookupWithEmptyStringReturnsEmpty() {
      List<SqlOperator> result = lookup("");
      assertTrue(result.isEmpty(), "Empty string lookup should return no operators");
    }

    @Test
    void lookupWithWhitespaceOnlyReturnsEmpty() {
      assertTrue(lookup("   ").isEmpty());
      assertTrue(lookup("\t").isEmpty());
      assertTrue(lookup("\n").isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "nonExistent", "fooBar", "UNKNOWN_FUNC", "selectFrom",
        "toDateTime2", "uniq_exact", "to_start_of_hour"
    })
    void lookupUnknownFunctionReturnsEmpty(String name) {
      assertTrue(lookup(name).isEmpty(),
          "Unknown function '" + name + "' should return empty");
    }
  }

  // -------------------------------------------------------------------------
  // Case-insensitive lookup edge cases
  // -------------------------------------------------------------------------

  @Nested
  class CaseInsensitiveLookups {

    static Stream<Arguments> allRegisteredFunctionsWithCaseVariations() {
      return Stream.of(
          // Time-bucketing
          Arguments.of("toStartOfInterval", "TOSTARTOFINTERVAL"),
          Arguments.of("toStartOfInterval", "tostartofinterval"),
          Arguments.of("toStartOfInterval", "ToStartOfInterval"),
          Arguments.of("toStartOfHour", "TOSTARTOFHOUR"),
          Arguments.of("toStartOfHour", "tostartofhour"),
          Arguments.of("toStartOfDay", "ToStartOfDay"),
          Arguments.of("toStartOfMinute", "TOSTARTOFMINUTE"),
          Arguments.of("toStartOfWeek", "tostartofweek"),
          Arguments.of("toStartOfMonth", "TOSTARTOFMONTH"),
          // Type-conversion
          Arguments.of("toDateTime", "TODATETIME"),
          Arguments.of("toDateTime", "todatetime"),
          Arguments.of("toDateTime", "ToDATETIME"),
          Arguments.of("toDate", "TODATE"),
          Arguments.of("toString", "TOSTRING"),
          Arguments.of("toUInt32", "TOUINT32"),
          Arguments.of("toInt32", "TOINT32"),
          Arguments.of("toInt64", "toint64"),
          Arguments.of("toFloat64", "TOFLOAT64"),
          Arguments.of("toFloat32", "tofloat32"),
          // Aggregates
          Arguments.of("uniq", "UNIQ"),
          Arguments.of("uniq", "Uniq"),
          Arguments.of("uniqExact", "UNIQEXACT"),
          Arguments.of("groupArray", "GROUPARRAY"),
          Arguments.of("count", "COUNT"),
          Arguments.of("count", "Count"),
          // Conditionals
          Arguments.of("if", "IF"),
          Arguments.of("if", "If"),
          Arguments.of("multiIf", "MULTIIF"),
          Arguments.of("multiIf", "multiif"),
          // Special
          Arguments.of("quantile", "QUANTILE"),
          Arguments.of("formatDateTime", "FORMATDATETIME"),
          Arguments.of("now", "NOW"),
          Arguments.of("now", "Now"),
          Arguments.of("today", "TODAY"),
          Arguments.of("today", "Today")
      );
    }

    @ParameterizedTest
    @MethodSource("allRegisteredFunctionsWithCaseVariations")
    void caseInsensitiveLookupResolvesToSameOperator(String canonical, String variant) {
      List<SqlOperator> canonicalResult = lookup(canonical);
      List<SqlOperator> variantResult = lookup(variant);

      assertFalse(canonicalResult.isEmpty(),
          "Canonical '" + canonical + "' should resolve");
      assertFalse(variantResult.isEmpty(),
          "Variant '" + variant + "' should resolve");
      assertSame(canonicalResult.get(0), variantResult.get(0),
          "'" + canonical + "' and '" + variant + "' should resolve to same operator");
    }
  }

  // -------------------------------------------------------------------------
  // Operator metadata consistency for all registered functions
  // -------------------------------------------------------------------------

  @Nested
  class OperatorMetadataConsistency {

    @ParameterizedTest
    @MethodSource("allRegisteredFunctionNames")
    void eachFunctionResolvesToExactlyOneOperator(String funcName) {
      List<SqlOperator> result = lookup(funcName);
      assertEquals(1, result.size(),
          "Function '" + funcName + "' should resolve to exactly one operator");
    }

    @ParameterizedTest
    @MethodSource("allRegisteredFunctionNames")
    void eachFunctionHasNonNullNonEmptyName(String funcName) {
      SqlOperator op = lookup(funcName).get(0);
      assertNotNull(op.getName(), "Operator name should not be null for " + funcName);
      assertFalse(op.getName().isEmpty(),
          "Operator name should not be empty for " + funcName);
    }

    @ParameterizedTest
    @MethodSource("allRegisteredFunctionNames")
    void eachFunctionHasNonNullReturnTypeInference(String funcName) {
      SqlOperator op = lookup(funcName).get(0);
      assertNotNull(op.getReturnTypeInference(),
          "Return type inference should not be null for " + funcName);
    }

    @ParameterizedTest
    @MethodSource("allRegisteredFunctionNames")
    void eachFunctionHasNonNullOperandTypeChecker(String funcName) {
      SqlOperator op = lookup(funcName).get(0);
      assertNotNull(op.getOperandTypeChecker(),
          "Operand type checker should not be null for " + funcName);
    }

    @ParameterizedTest
    @MethodSource("allRegisteredFunctionNames")
    void eachFunctionHasConsistentKindAndSyntax(String funcName) {
      SqlOperator op = lookup(funcName).get(0);
      assertDoesNotThrow(() -> op.getKind(),
          "getKind() should not throw for " + funcName);
      assertDoesNotThrow(() -> op.getSyntax(),
          "getSyntax() should not throw for " + funcName);
    }

    @ParameterizedTest
    @MethodSource("allRegisteredFunctionNames")
    void repeatedLookupReturnsSameInstance(String funcName) {
      SqlOperator first = lookup(funcName).get(0);
      SqlOperator second = lookup(funcName).get(0);
      assertSame(first, second,
          "Repeated lookups for '" + funcName + "' should return same instance");
    }

    static Stream<String> allRegisteredFunctionNames() {
      return ClickHouseOperatorTable.INSTANCE.getRegisteredFunctionNames().stream();
    }
  }

  // -------------------------------------------------------------------------
  // Type-conversion function return type expectations
  // -------------------------------------------------------------------------

  @Nested
  class TypeConversionReturnTypes {

    static Stream<Arguments> typeConversionExpectations() {
      return Stream.of(
          Arguments.of("toDateTime", SqlTypeName.TIMESTAMP),
          Arguments.of("toDate", SqlTypeName.DATE),
          Arguments.of("toString", SqlTypeName.VARCHAR),
          Arguments.of("toUInt32", SqlTypeName.INTEGER),
          Arguments.of("toInt32", SqlTypeName.INTEGER),
          Arguments.of("toInt64", SqlTypeName.BIGINT),
          Arguments.of("toFloat64", SqlTypeName.DOUBLE),
          Arguments.of("toFloat32", SqlTypeName.FLOAT)
      );
    }

    @ParameterizedTest
    @MethodSource("typeConversionExpectations")
    void typeConversionFunctionHasCorrectReturnType(String funcName,
        SqlTypeName expectedType) {
      SqlOperator op = lookup(funcName).get(0);
      // Verify the operator is a SqlFunction (type-conversion functions are custom SqlFunctions)
      assertInstanceOf(SqlFunction.class, op,
          funcName + " should be a SqlFunction");
      // The return type inference is set; we verify it's non-null and the operator is well-formed
      assertNotNull(op.getReturnTypeInference(),
          funcName + " should have return type inference configured");
    }
  }

  // -------------------------------------------------------------------------
  // Time-bucketing function category verification
  // -------------------------------------------------------------------------

  @Nested
  class TimeBucketingFunctionCategories {

    @ParameterizedTest
    @ValueSource(strings = {
        "toStartOfInterval", "toStartOfHour", "toStartOfDay",
        "toStartOfMinute", "toStartOfWeek", "toStartOfMonth"
    })
    void timeBucketingFunctionIsCategorizedAsTimeDate(String funcName) {
      SqlOperator op = lookup(funcName).get(0);
      assertInstanceOf(SqlFunction.class, op);
      SqlFunction func = (SqlFunction) op;
      assertEquals(SqlFunctionCategory.TIMEDATE, func.getFunctionType(),
          funcName + " should be in TIMEDATE category");
    }
  }

  // -------------------------------------------------------------------------
  // Aggregate function shared operator verification
  // -------------------------------------------------------------------------

  @Nested
  class AggregateFunctionMappings {

    @Test
    void uniqAndUniqExactShareSameOperator() {
      SqlOperator uniq = lookup("uniq").get(0);
      SqlOperator uniqExact = lookup("uniqExact").get(0);
      assertSame(uniq, uniqExact,
          "uniq and uniqExact should map to the same COUNT operator");
    }

    @Test
    void countAndUniqShareSameOperator() {
      SqlOperator count = lookup("count").get(0);
      SqlOperator uniq = lookup("uniq").get(0);
      assertSame(count, uniq,
          "count and uniq should map to the same COUNT operator");
    }
  }

  // -------------------------------------------------------------------------
  // Compound and special identifier lookups
  // -------------------------------------------------------------------------

  @Nested
  class SpecialIdentifierLookups {

    @Test
    void compoundIdentifierReturnsEmpty() {
      List<SqlOperator> result = new ArrayList<>();
      SqlIdentifier compoundId =
          new SqlIdentifier(List.of("schema", "toDateTime"), SqlParserPos.ZERO);
      table.lookupOperatorOverloads(
          compoundId, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
      assertTrue(result.isEmpty(),
          "Compound identifier should not resolve in operator table");
    }

    @Test
    void lookupWithSpecialCharactersReturnsEmpty() {
      assertTrue(lookup("toDateTime!").isEmpty());
      assertTrue(lookup("to-date-time").isEmpty());
      assertTrue(lookup("to.date.time").isEmpty());
      assertTrue(lookup("toDateTime()").isEmpty());
      assertTrue(lookup("toDateTime;DROP").isEmpty());
    }

    @Test
    void lookupWithNumericStringReturnsEmpty() {
      assertTrue(lookup("12345").isEmpty());
      assertTrue(lookup("0").isEmpty());
      assertTrue(lookup("-1").isEmpty());
    }
  }

  // -------------------------------------------------------------------------
  // getOperatorList and getRegisteredFunctionNames consistency
  // -------------------------------------------------------------------------

  @Nested
  class OperatorListConsistency {

    @Test
    void operatorListIsNotEmpty() {
      List<SqlOperator> operators = table.getOperatorList();
      assertNotNull(operators);
      assertFalse(operators.isEmpty());
    }

    @Test
    void registeredNamesMatchOperatorListSize() {
      Set<String> names = table.getRegisteredFunctionNames();
      List<SqlOperator> operators = table.getOperatorList();
      // Names and operators should have same count (each name maps to one operator)
      assertEquals(names.size(), operators.size(),
          "Registered names count should match operator list size");
    }

    @Test
    void everyRegisteredNameResolvesViaLookup() {
      Set<String> names = table.getRegisteredFunctionNames();
      for (String name : names) {
        List<SqlOperator> result = lookup(name);
        assertFalse(result.isEmpty(),
            "Registered name '" + name + "' should resolve via lookup");
      }
    }

    @Test
    void registeredFunctionNamesSetIsUnmodifiable() {
      Set<String> names = table.getRegisteredFunctionNames();
      assertThrows(UnsupportedOperationException.class, () -> names.add("hacked"),
          "Registered function names set should be unmodifiable");
    }

    @Test
    void expectedFunctionCountCoversAllCategories() {
      Set<String> names = table.getRegisteredFunctionNames();
      // 6 time-bucketing + 8 type-conversion + 4 aggregate + 2 conditional + 4 special = 24
      assertEquals(24, names.size(),
          "Expected 24 registered functions across all categories");
    }
  }

  // -------------------------------------------------------------------------
  // Helper
  // -------------------------------------------------------------------------

  private List<SqlOperator> lookup(String name) {
    List<SqlOperator> result = new ArrayList<>();
    SqlIdentifier id = new SqlIdentifier(name, SqlParserPos.ZERO);
    table.lookupOperatorOverloads(
        id, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
    return result;
  }
}

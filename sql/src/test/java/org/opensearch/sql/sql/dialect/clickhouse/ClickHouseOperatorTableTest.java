/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Unit tests for {@link ClickHouseOperatorTable}. */
class ClickHouseOperatorTableTest {

  private final ClickHouseOperatorTable table = ClickHouseOperatorTable.INSTANCE;

  @Test
  void singletonInstance() {
    assertSame(ClickHouseOperatorTable.INSTANCE, ClickHouseOperatorTable.INSTANCE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "toStartOfInterval",
        "toStartOfHour",
        "toStartOfDay",
        "toStartOfMinute",
        "toStartOfWeek",
        "toStartOfMonth"
      })
  void timeBucketingFunctionsRegistered(String funcName) {
    List<SqlOperator> result = lookup(funcName);
    assertFalse(result.isEmpty(), "Expected operator for " + funcName);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "toDateTime",
        "toDate",
        "toString",
        "toUInt32",
        "toInt32",
        "toInt64",
        "toFloat64",
        "toFloat32"
      })
  void typeConversionFunctionsRegistered(String funcName) {
    List<SqlOperator> result = lookup(funcName);
    assertFalse(result.isEmpty(), "Expected operator for " + funcName);
  }

  @ParameterizedTest
  @ValueSource(strings = {"uniq", "uniqExact", "groupArray", "count"})
  void aggregateFunctionsRegistered(String funcName) {
    List<SqlOperator> result = lookup(funcName);
    assertFalse(result.isEmpty(), "Expected operator for " + funcName);
  }

  @ParameterizedTest
  @ValueSource(strings = {"if", "multiIf"})
  void conditionalFunctionsRegistered(String funcName) {
    List<SqlOperator> result = lookup(funcName);
    assertFalse(result.isEmpty(), "Expected operator for " + funcName);
  }

  @ParameterizedTest
  @ValueSource(strings = {"quantile", "formatDateTime", "now", "today"})
  void specialFunctionsRegistered(String funcName) {
    List<SqlOperator> result = lookup(funcName);
    assertFalse(result.isEmpty(), "Expected operator for " + funcName);
  }

  @Test
  void lookupIsCaseInsensitive() {
    assertFalse(lookup("TODATETIME").isEmpty());
    assertFalse(lookup("todatetime").isEmpty());
    assertFalse(lookup("ToDateTime").isEmpty());
  }

  @Test
  void lookupUnregisteredFunctionReturnsEmpty() {
    assertTrue(lookup("nonExistentFunction").isEmpty());
  }

  @Test
  void getOperatorListReturnsAllRegistered() {
    List<SqlOperator> operators = table.getOperatorList();
    assertNotNull(operators);
    assertFalse(operators.isEmpty());
  }

  @Test
  void getRegisteredFunctionNamesContainsExpectedNames() {
    Set<String> names = table.getRegisteredFunctionNames();
    assertTrue(names.contains("now"));
    assertTrue(names.contains("today"));
    assertTrue(names.contains("todatetime"));
    assertTrue(names.contains("uniq"));
    assertTrue(names.contains("if"));
    assertTrue(names.contains("tostartofhour"));
    assertTrue(names.contains("quantile"));
    assertTrue(names.contains("formatdatetime"));
    assertTrue(names.contains("grouparray"));
    assertTrue(names.contains("count"));
  }

  @Test
  void uniqAndUniqExactMapToSameOperator() {
    List<SqlOperator> uniq = lookup("uniq");
    List<SqlOperator> uniqExact = lookup("uniqExact");
    assertEquals(1, uniq.size());
    assertEquals(1, uniqExact.size());
    assertSame(uniq.get(0), uniqExact.get(0));
  }

  @Test
  void compoundIdentifierNotLookedUp() {
    List<SqlOperator> result = new ArrayList<>();
    SqlIdentifier compoundId =
        new SqlIdentifier(List.of("schema", "toDateTime"), SqlParserPos.ZERO);
    table.lookupOperatorOverloads(
        compoundId, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
    assertTrue(result.isEmpty());
  }

  private List<SqlOperator> lookup(String name) {
    List<SqlOperator> result = new ArrayList<>();
    SqlIdentifier id = new SqlIdentifier(name, SqlParserPos.ZERO);
    table.lookupOperatorOverloads(
        id, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
    return result;
  }
}

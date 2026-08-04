/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.DateOnlyType;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.analytics.schema.TimeOnlyType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

class PPLTypeCheckerTest {

  private static final OpenSearchTypeFactory TYPE_FACTORY = OpenSearchTypeFactory.TYPE_FACTORY;

  @Test
  void matchesAnalyticsTypesToEquivalentPplUdts() {
    assertMatches(ExprUDT.EXPR_TIMESTAMP, TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP));
    assertMatches(ExprUDT.EXPR_DATE, new DateOnlyType(RelDataTypeSystem.DEFAULT, true, 3));
    assertMatches(ExprUDT.EXPR_TIME, new TimeOnlyType(RelDataTypeSystem.DEFAULT, true, 3));
    assertMatches(ExprUDT.EXPR_IP, new IpType(true));
    assertMatches(ExprUDT.EXPR_BINARY, new BinaryType(true));
  }

  @Test
  void doesNotConflateDifferentSemanticTypes() {
    assertDoesNotMatch(
        ExprUDT.EXPR_TIMESTAMP, new DateOnlyType(RelDataTypeSystem.DEFAULT, true, 3));
    assertDoesNotMatch(ExprUDT.EXPR_IP, new BinaryType(true));
    assertDoesNotMatch(
        TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP),
        new DateOnlyType(RelDataTypeSystem.DEFAULT, true, 3));
    assertDoesNotMatch(TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY), new IpType(true));
  }

  private static void assertMatches(ExprUDT expected, RelDataType actual) {
    assertTrue(checker(TYPE_FACTORY.createUDT(expected)).checkOperandTypes(List.of(actual)));
  }

  private static void assertDoesNotMatch(ExprUDT expected, RelDataType actual) {
    assertDoesNotMatch(TYPE_FACTORY.createUDT(expected), actual);
  }

  private static void assertDoesNotMatch(RelDataType expected, RelDataType actual) {
    assertFalse(checker(expected).checkOperandTypes(List.of(actual)));
  }

  private static PPLTypeChecker checker(RelDataType expected) {
    return PPLTypeChecker.wrapUDT(List.of(List.of(expected)));
  }
}

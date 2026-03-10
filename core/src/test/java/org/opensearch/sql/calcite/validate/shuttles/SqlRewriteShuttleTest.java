/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.shuttles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.OpenSearchSchema;

public class SqlRewriteShuttleTest {

  private SqlRewriteShuttle shuttle;

  @BeforeEach
  public void setUp() {
    shuttle = new SqlRewriteShuttle();
  }

  @Test
  public void testVisitIdentifierRemovesOpenSearchQualifier() {
    // Create qualified identifier: OpenSearch.tableName
    SqlIdentifier qualifiedId =
        new SqlIdentifier(
            Arrays.asList(OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, "my_table"), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(qualifiedId);

    assertInstanceOf(SqlIdentifier.class, result);
    SqlIdentifier resultId = (SqlIdentifier) result;
    assertEquals(1, resultId.names.size());
    assertEquals("my_table", resultId.names.get(0));
  }

  @Test
  public void testVisitIdentifierKeepsOtherQualifiers() {
    // Create qualified identifier with different schema: OtherSchema.tableName
    SqlIdentifier qualifiedId =
        new SqlIdentifier(Arrays.asList("OtherSchema", "my_table"), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(qualifiedId);

    assertInstanceOf(SqlIdentifier.class, result);
    SqlIdentifier resultId = (SqlIdentifier) result;
    assertEquals(2, resultId.names.size());
    assertEquals("OtherSchema", resultId.names.get(0));
    assertEquals("my_table", resultId.names.get(1));
  }

  @Test
  public void testVisitIdentifierSinglePartUnchanged() {
    // Create single-part identifier: tableName
    SqlIdentifier simpleId =
        new SqlIdentifier(Collections.singletonList("my_table"), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(simpleId);

    assertInstanceOf(SqlIdentifier.class, result);
    SqlIdentifier resultId = (SqlIdentifier) result;
    assertEquals(1, resultId.names.size());
    assertEquals("my_table", resultId.names.get(0));
  }

  @Test
  public void testVisitIdentifierThreePartsUnchanged() {
    // Create three-part identifier: catalog.schema.table
    SqlIdentifier threePartId =
        new SqlIdentifier(
            Arrays.asList(OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, "schema", "table"),
            SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(threePartId);

    // Should not be modified because it has 3 parts, not 2
    assertInstanceOf(SqlIdentifier.class, result);
    SqlIdentifier resultId = (SqlIdentifier) result;
    assertEquals(3, resultId.names.size());
  }

  @Test
  public void testVisitCallCountEmptyToCountStar() {
    // Create COUNT() call with no operands
    SqlCountAggFunction countFunction = new SqlCountAggFunction("COUNT");
    SqlBasicCall countCall = new SqlBasicCall(countFunction, List.of(), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(countCall);

    assertInstanceOf(SqlBasicCall.class, result);
    SqlBasicCall resultCall = (SqlBasicCall) result;
    assertEquals(1, resultCall.getOperandList().size());
    assertInstanceOf(SqlIdentifier.class, resultCall.getOperandList().get(0));
    SqlIdentifier operand = (SqlIdentifier) resultCall.getOperandList().get(0);
    assertTrue(operand.isStar());
  }

  @Test
  public void testVisitCallCountWithOperandUnchanged() {
    // Create COUNT(column) call - should not be converted to COUNT(*)
    SqlCountAggFunction countFunction = new SqlCountAggFunction("COUNT");
    SqlIdentifier column = new SqlIdentifier("my_column", SqlParserPos.ZERO);
    SqlBasicCall countCall = new SqlBasicCall(countFunction, List.of(column), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(countCall);

    assertInstanceOf(SqlBasicCall.class, result);
    SqlBasicCall resultCall = (SqlBasicCall) result;
    assertEquals(1, resultCall.getOperandList().size());
    assertInstanceOf(SqlIdentifier.class, resultCall.getOperandList().get(0));
    SqlIdentifier operand = (SqlIdentifier) resultCall.getOperandList().get(0);
    assertEquals("my_column", operand.getSimple());
  }

  @Test
  public void testVisitCallInWithSqlNodeListWrapsInRow() {
    // Create IN call with SqlNodeList as first operand
    SqlIdentifier id1 = new SqlIdentifier("col1", SqlParserPos.ZERO);
    SqlIdentifier id2 = new SqlIdentifier("col2", SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(id1, id2), SqlParserPos.ZERO);

    SqlIdentifier subquery = new SqlIdentifier("subquery", SqlParserPos.ZERO);
    SqlBasicCall inCall =
        new SqlBasicCall(SqlStdOperatorTable.IN, List.of(nodeList, subquery), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(inCall);

    assertInstanceOf(SqlBasicCall.class, result);
    SqlBasicCall resultCall = (SqlBasicCall) result;
    assertEquals(SqlKind.IN, resultCall.getKind());
    // First operand should now be a ROW call
    assertInstanceOf(SqlBasicCall.class, resultCall.getOperandList().get(0));
    SqlBasicCall rowCall = (SqlBasicCall) resultCall.getOperandList().get(0);
    assertEquals(SqlKind.ROW, rowCall.getKind());
  }

  @Test
  public void testVisitCallNotInWithSqlNodeListWrapsInRow() {
    // Create NOT IN call with SqlNodeList as first operand
    SqlIdentifier id1 = new SqlIdentifier("col1", SqlParserPos.ZERO);
    SqlIdentifier id2 = new SqlIdentifier("col2", SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(id1, id2), SqlParserPos.ZERO);

    SqlIdentifier subquery = new SqlIdentifier("subquery", SqlParserPos.ZERO);
    SqlBasicCall notInCall =
        new SqlBasicCall(
            SqlStdOperatorTable.NOT_IN, List.of(nodeList, subquery), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(notInCall);

    assertInstanceOf(SqlBasicCall.class, result);
    SqlBasicCall resultCall = (SqlBasicCall) result;
    assertEquals(SqlKind.NOT_IN, resultCall.getKind());
    // First operand should now be a ROW call
    assertInstanceOf(SqlBasicCall.class, resultCall.getOperandList().get(0));
    SqlBasicCall rowCall = (SqlBasicCall) resultCall.getOperandList().get(0);
    assertEquals(SqlKind.ROW, rowCall.getKind());
  }

  @Test
  public void testVisitCallInWithNonSqlNodeListUnchanged() {
    // Create IN call with regular SqlIdentifier as first operand (not SqlNodeList)
    SqlIdentifier column = new SqlIdentifier("my_column", SqlParserPos.ZERO);
    SqlIdentifier subquery = new SqlIdentifier("subquery", SqlParserPos.ZERO);
    SqlBasicCall inCall =
        new SqlBasicCall(SqlStdOperatorTable.IN, List.of(column, subquery), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(inCall);

    assertInstanceOf(SqlBasicCall.class, result);
    SqlBasicCall resultCall = (SqlBasicCall) result;
    assertEquals(SqlKind.IN, resultCall.getKind());
    // First operand should still be SqlIdentifier, not wrapped in ROW
    assertInstanceOf(SqlIdentifier.class, resultCall.getOperandList().get(0));
  }

  @Test
  public void testVisitCallOtherOperatorUnchanged() {
    // Create a regular binary operation like +
    SqlIdentifier left = new SqlIdentifier("col1", SqlParserPos.ZERO);
    SqlIdentifier right = new SqlIdentifier("col2", SqlParserPos.ZERO);
    SqlBasicCall plusCall =
        new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(left, right), SqlParserPos.ZERO);

    SqlNode result = shuttle.visit(plusCall);

    assertInstanceOf(SqlBasicCall.class, result);
    SqlBasicCall resultCall = (SqlBasicCall) result;
    assertEquals(SqlKind.PLUS, resultCall.getKind());
  }
}

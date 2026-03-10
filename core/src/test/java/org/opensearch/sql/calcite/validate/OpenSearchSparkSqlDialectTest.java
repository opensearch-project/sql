/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class OpenSearchSparkSqlDialectTest {
  @Test
  public void testGetCastSpecForIpType() {
    RelDataType ipType = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    SqlNode castSpec = OpenSearchSparkSqlDialect.DEFAULT.getCastSpec(ipType);

    assertNotNull(castSpec);
    assertInstanceOf(SqlDataTypeSpec.class, castSpec);
    SqlDataTypeSpec typeSpec = (SqlDataTypeSpec) castSpec;
    assertEquals("OTHER", typeSpec.getTypeName().toString());
    assertEquals("IP", typeSpec.toString());
  }

  private SqlWriter createWriter(StringBuilder sb) {
    SqlWriterConfig config =
        SqlPrettyWriter.config().withDialect(OpenSearchSparkSqlDialect.DEFAULT);
    return new SqlPrettyWriter(config, sb);
  }

  @Test
  public void testUnparseCallArgMin() {
    StringBuilder sb = new StringBuilder();
    SqlWriter writer = createWriter(sb);

    SqlIdentifier col1 = new SqlIdentifier("value_col", SqlParserPos.ZERO);
    SqlIdentifier col2 = new SqlIdentifier("key_col", SqlParserPos.ZERO);

    // Create a call that mimics ARG_MIN
    SqlBasicCall argMinCall =
        new SqlBasicCall(SqlStdOperatorTable.ARG_MIN, List.of(col1, col2), SqlParserPos.ZERO);

    OpenSearchSparkSqlDialect.DEFAULT.unparseCall(writer, argMinCall, 0, 0);
    String result = sb.toString();

    // Should be translated to MIN_BY
    assertTrue(result.contains("MIN_BY"));
  }

  @Test
  public void testUnparseCallArgMax() {
    StringBuilder sb = new StringBuilder();
    SqlWriter writer = createWriter(sb);

    SqlIdentifier col1 = new SqlIdentifier("value_col", SqlParserPos.ZERO);
    SqlIdentifier col2 = new SqlIdentifier("key_col", SqlParserPos.ZERO);

    SqlBasicCall argMaxCall =
        new SqlBasicCall(SqlStdOperatorTable.ARG_MAX, List.of(col1, col2), SqlParserPos.ZERO);

    OpenSearchSparkSqlDialect.DEFAULT.unparseCall(writer, argMaxCall, 0, 0);
    String result = sb.toString();

    // Should be translated to MAX_BY
    assertTrue(result.contains("MAX_BY"));
  }

  @Test
  public void testUnparseCallRegularOperator() {
    StringBuilder sb = new StringBuilder();
    SqlWriter writer = createWriter(sb);

    SqlIdentifier col1 = new SqlIdentifier("col1", SqlParserPos.ZERO);
    SqlIdentifier col2 = new SqlIdentifier("col2", SqlParserPos.ZERO);

    SqlBasicCall plusCall =
        new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(col1, col2), SqlParserPos.ZERO);

    OpenSearchSparkSqlDialect.DEFAULT.unparseCall(writer, plusCall, 0, 0);
    String result = sb.toString();

    // Should contain the + operator, not translated
    assertTrue(result.contains("+"));
  }

  @Test
  public void testGetConformanceIsLiberal() {
    SqlConformance conformance = OpenSearchSparkSqlDialect.DEFAULT.getConformance();

    assertNotNull(conformance);
    assertTrue(conformance.isLiberal());
  }

  @Test
  public void testGetCastSpecForNonIpType() {
    // Non-IP types should delegate to parent SparkSqlDialect
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    SqlNode castSpec = OpenSearchSparkSqlDialect.DEFAULT.getCastSpec(intType);

    // SparkSqlDialect returns a SqlDataTypeSpec for INTEGER type
    assertNotNull(castSpec);
    assertInstanceOf(SqlDataTypeSpec.class, castSpec);
    // It should NOT be the IP-specific spec
    SqlDataTypeSpec typeSpec = (SqlDataTypeSpec) castSpec;
    assertEquals("INTEGER", typeSpec.toString());
  }

  @Test
  public void testGetCastSpecForVarcharType() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    SqlNode castSpec = OpenSearchSparkSqlDialect.DEFAULT.getCastSpec(varcharType);

    // SparkSqlDialect handles VARCHAR specially, returns a SqlDataTypeSpec
    assertNotNull(castSpec);
    assertInstanceOf(SqlDataTypeSpec.class, castSpec);
  }

  @Test
  public void testGetCastSpecForNullType() {
    // Null input should throw NullPointerException
    assertThrows(
        NullPointerException.class, () -> OpenSearchSparkSqlDialect.DEFAULT.getCastSpec(null));
  }
}

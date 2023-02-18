/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.jdbc.functions.JDBCTableFunctionResolver.JDBC_FUNCTION_NAME;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

@ExtendWith(MockitoExtension.class)
class JDBCFunctionTest {
  private final String sql = "select * from table";

  @Mock
  private Environment<Expression, ExprValue> valueEnv;

  @Mock
  private TableScanOperator scanOperator;

  @Test
  public void valueOf() {
    JDBCFunction function = new JDBCFunction(JDBC_FUNCTION_NAME, sql, new Properties());

    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> function.valueOf(valueEnv));
    assertEquals("JDBC function is only supported in source command", exception.getMessage());
  }

  @Test
  public void type() {
    JDBCFunction function = new JDBCFunction(JDBC_FUNCTION_NAME, sql, new Properties());
    assertEquals(STRUCT, function.type());
  }

  @Test
  public void applyArguments() {
    JDBCFunction function = new JDBCFunction(JDBC_FUNCTION_NAME, sql, new Properties());
    Table table = function.applyArguments();

    assertTrue(table instanceof JDBCFunction.JDBCFunctionTable);
  }

  @Test
  public void jdbcTableFunctionGetFieldTypes() {
    JDBCFunction function = new JDBCFunction(JDBC_FUNCTION_NAME, sql, new Properties());
    Table table = function.applyArguments();

    assertTrue(table.getFieldTypes().isEmpty());
  }

  @Test
  public void jdbcTableFunctionImplement() {
    JDBCFunction function = new JDBCFunction(JDBC_FUNCTION_NAME, sql, new Properties());
    Table table = function.applyArguments();

    assertEquals(scanOperator, table.implement(new TableScanBuilder() {
      @Override
      public TableScanOperator build() {
        return scanOperator;
      }
    }));
  }

  @Test
  public void jdbcTableFunctionCreateScanBuilder() {
    JDBCFunction function = new JDBCFunction(JDBC_FUNCTION_NAME, sql, new Properties());
    Table table = function.applyArguments();

    assertTrue(table.createScanBuilder() instanceof JDBCFunction.JDBCFunctionTableScanBuilder);
  }

  @Test
  public void jdbcFunctionTableScanBuilderBuild() {
    TableScanBuilder scanBuilder =
        new JDBCFunction(JDBC_FUNCTION_NAME, sql, new Properties()).applyArguments()
            .createScanBuilder();
    assertNotNull(scanBuilder.build());
  }

  @Test
  public void jdbcFunctionTableScanBuilderPushDownProject() {
    TableScanBuilder scanBuilder =
        new JDBCFunction(JDBC_FUNCTION_NAME, sql, new Properties()).applyArguments()
            .createScanBuilder();
    assertTrue(scanBuilder.pushDownProject(mock(LogicalProject.class)));
  }
}

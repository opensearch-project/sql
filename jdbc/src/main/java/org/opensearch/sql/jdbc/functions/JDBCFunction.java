/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.functions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.jdbc.operator.JDBCQueryOperator;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * JDBC function definition.
 */
public class JDBCFunction extends FunctionExpression implements TableFunctionImplementation {

  private final String sqlQuery;

  private final Properties properties;

  /**
   * constructor.
   */
  public JDBCFunction(
      FunctionName functionName, String sqlQuery, Properties properties) {
    super(functionName, List.of());
    this.sqlQuery = sqlQuery;
    this.properties = properties;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new SemanticCheckException("JDBC function is only supported in source command");
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRUCT;
  }

  @Override
  public Table applyArguments() {
    return new JDBCFunctionTable();
  }

  /**
   * The table created from {@link JDBCFunction}.
   */
  @VisibleForTesting
  protected class JDBCFunctionTable implements Table {
    /**
     * return empty map at query analysis stage.
     */
    @Override
    public Map<String, ExprType> getFieldTypes() {
      return ImmutableMap.of();
    }

    // todo, the implement interface should be removed. https://github.com/opensearch-project/sql/issues/1463
    @SuppressWarnings("deprecation")
    @Override
    public PhysicalPlan implement(LogicalPlan plan) {
      return plan.accept(new DefaultImplementor<>(), null);
    }

    @Override
    public TableScanBuilder createScanBuilder() {
      return new JDBCFunctionTableScanBuilder();
    }
  }

  /**
   * {@link JDBCFunctionTable} scan builder.
   */
  @VisibleForTesting
  protected class JDBCFunctionTableScanBuilder extends TableScanBuilder {

    @Override
    public TableScanOperator build() {
      return new JDBCQueryOperator(sqlQuery, properties);
    }

    /**
     * PPL by default add a LogicalProject operator. It should be ignored.
     */
    @Override
    public boolean pushDownProject(LogicalProject project) {
      return true;
    }
  }
}

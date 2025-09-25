/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.util.List;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.utils.Util;

/** Created by Eliran on 20/8/2015. */
public class JoinSelect extends Query {

  private final TableOnJoinSelect firstTable;
  private final TableOnJoinSelect secondTable;
  private Where connectedWhere;
  private List<Hint> hints;
  private List<Condition> connectedConditions;
  private int totalLimit;

  private final int DEAFULT_NUM_OF_RESULTS = 200;

  private SQLJoinTableSource.JoinType joinType;

  public JoinSelect() {
    firstTable = new TableOnJoinSelect();
    secondTable = new TableOnJoinSelect();

    totalLimit = DEAFULT_NUM_OF_RESULTS;
  }

  /**
   * Creates a JoinSelect with validation to ensure JOIN queries do not use aggregations. This
   * factory method guarantees that JOIN aggregation validation is applied everywhere JoinSelect is
   * constructed from a query.
   *
   * @param query The select query block to validate
   * @return A new JoinSelect instance
   * @throws SqlParseException if the query contains aggregations (GROUP BY or aggregate functions)
   */
  public static JoinSelect withValidation(MySqlSelectQueryBlock query) throws SqlParseException {
    validateJoinWithoutAggregations(query);
    return new JoinSelect();
  }

  /**
   * Validates that JOIN queries do not use aggregations (GROUP BY or aggregate functions). This
   * limitation is documented in the official OpenSearch SQL documentation.
   */
  private static void validateJoinWithoutAggregations(MySqlSelectQueryBlock query)
      throws SqlParseException {
    String errorMessage =
        Util.JOIN_AGGREGATION_ERROR_PREFIX
            + Util.DOC_REDIRECT_MESSAGE
            + Util.getJoinAggregationDocumentationUrl(JoinSelect.class);

    if (query.getGroupBy() != null && !query.getGroupBy().getItems().isEmpty()) {
      throw new SqlParseException(errorMessage);
    }

    if (query.getSelectList() != null) {
      for (SQLSelectItem selectItem : query.getSelectList()) {
        if (containsAggregateFunction(selectItem.getExpr())) {
          throw new SqlParseException(errorMessage);
        }
      }
    }
  }

  /**
   * Recursively checks if an SQL expression contains aggregate functions. Uses the same
   * AGGREGATE_FUNCTIONS set as the Select class for consistency.
   */
  private static boolean containsAggregateFunction(SQLExpr expr) {
    if (expr == null) {
      return false;
    }

    String methodName = null;
    if (expr instanceof SQLAggregateExpr) {
      methodName = ((SQLAggregateExpr) expr).getMethodName();
    } else if (expr instanceof SQLMethodInvokeExpr) {
      methodName = ((SQLMethodInvokeExpr) expr).getMethodName();
    }

    if (methodName != null && Select.AGGREGATE_FUNCTIONS.contains(methodName.toUpperCase())) {
      return true;
    }

    if (expr instanceof SQLBinaryOpExpr) {
      SQLBinaryOpExpr binaryExpr = (SQLBinaryOpExpr) expr;
      return containsAggregateFunction(binaryExpr.getLeft())
          || containsAggregateFunction(binaryExpr.getRight());
    }

    return false;
  }

  public Where getConnectedWhere() {
    return connectedWhere;
  }

  public void setConnectedWhere(Where connectedWhere) {
    this.connectedWhere = connectedWhere;
  }

  public TableOnJoinSelect getFirstTable() {
    return firstTable;
  }

  public TableOnJoinSelect getSecondTable() {
    return secondTable;
  }

  public SQLJoinTableSource.JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(SQLJoinTableSource.JoinType joinType) {
    this.joinType = joinType;
  }

  public List<Hint> getHints() {
    return hints;
  }

  public void setHints(List<Hint> hints) {
    this.hints = hints;
  }

  public int getTotalLimit() {
    return totalLimit;
  }

  public List<Condition> getConnectedConditions() {
    return connectedConditions;
  }

  public void setConnectedConditions(List<Condition> connectedConditions) {
    this.connectedConditions = connectedConditions;
  }

  public void setTotalLimit(int totalLimit) {
    this.totalLimit = totalLimit;
  }
}

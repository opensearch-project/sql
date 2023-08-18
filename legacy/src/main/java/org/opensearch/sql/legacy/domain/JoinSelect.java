/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import java.util.List;
import org.opensearch.sql.legacy.domain.hints.Hint;

/** Created by Eliran on 20/8/2015. */
public class JoinSelect extends Query {

  private TableOnJoinSelect firstTable;
  private TableOnJoinSelect secondTable;
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

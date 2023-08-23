/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.subquery.utils;

import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.ArrayList;
import java.util.List;

/** Visitor which try to find the SubQuery. */
public class FindSubQuery extends MySqlASTVisitorAdapter {
  private final List<SQLInSubQueryExpr> sqlInSubQueryExprs = new ArrayList<>();
  private final List<SQLExistsExpr> sqlExistsExprs = new ArrayList<>();
  private boolean continueVisit = true;

  public FindSubQuery continueVisitWhenFound(boolean continueVisit) {
    this.continueVisit = continueVisit;
    return this;
  }

  /** Return true if has SubQuery. */
  public boolean hasSubQuery() {
    return !sqlInSubQueryExprs.isEmpty() || !sqlExistsExprs.isEmpty();
  }

  @Override
  public boolean visit(SQLInSubQueryExpr query) {
    sqlInSubQueryExprs.add(query);
    return continueVisit;
  }

  @Override
  public boolean visit(SQLExistsExpr query) {
    sqlExistsExprs.add(query);
    return continueVisit;
  }

  public List<SQLInSubQueryExpr> getSqlInSubQueryExprs() {
    return sqlInSubQueryExprs;
  }

  public List<SQLExistsExpr> getSqlExistsExprs() {
    return sqlExistsExprs;
  }
}

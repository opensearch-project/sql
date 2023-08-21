/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.identifier;

import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import org.opensearch.sql.legacy.rewriter.RewriteRule;

/**
 * Rewrite rule to anonymize sensitive data in logging queries. This rule replace the content of
 * specific nodes (that might involve index data) in AST to anonymous content.
 */
public class AnonymizeSensitiveDataRule extends MySqlASTVisitorAdapter
    implements RewriteRule<SQLQueryExpr> {

  @Override
  public boolean visit(SQLIdentifierExpr identifierExpr) {
    if (identifierExpr.getParent() instanceof SQLExprTableSource) {
      identifierExpr.setName("table");
    } else {
      identifierExpr.setName("identifier");
    }
    return true;
  }

  @Override
  public boolean visit(SQLIntegerExpr integerExpr) {
    integerExpr.setNumber(0);
    return true;
  }

  @Override
  public boolean visit(SQLNumberExpr numberExpr) {
    numberExpr.setNumber(0);
    return true;
  }

  @Override
  public boolean visit(SQLCharExpr charExpr) {
    charExpr.setText("string_literal");
    return true;
  }

  @Override
  public boolean visit(SQLBooleanExpr booleanExpr) {
    booleanExpr.setValue(false);
    return true;
  }

  @Override
  public boolean match(SQLQueryExpr expr) {
    return true;
  }

  @Override
  public void rewrite(SQLQueryExpr expr) {
    expr.accept(this);
  }
}

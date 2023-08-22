/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.parent;

import static org.junit.Assert.assertNotNull;

import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import org.junit.Test;
import org.opensearch.sql.legacy.util.SqlParserUtils;

public class SQLExprParentSetterTest {

  @Test
  public void testSQLInSubQueryExprHasParent() {
    String sql = "SELECT * FROM TbA " + "WHERE a IN (SELECT b FROM TbB)";
    SQLQueryExpr expr = SqlParserUtils.parse(sql);
    expr.accept(new SQLExprParentExistsValidator());
  }

  @Test
  public void testSQLInListExprHasParent() {
    String sql = "SELECT * FROM TbA " + "WHERE a IN (10, 20)";
    SQLQueryExpr expr = SqlParserUtils.parse(sql);
    expr.accept(new SQLExprParentExistsValidator());
  }

  static class SQLExprParentExistsValidator extends MySqlASTVisitorAdapter {
    @Override
    public boolean visit(SQLInSubQueryExpr expr) {
      assertNotNull(expr.getExpr().getParent());
      return true;
    }

    @Override
    public boolean visit(SQLInListExpr expr) {
      assertNotNull(expr.getExpr().getParent());
      return true;
    }
  }
}

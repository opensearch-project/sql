/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.parent;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.opensearch.sql.legacy.rewriter.RewriteRule;

/** The {@link RewriteRule} which will apply {@link SQLExprParentSetter} for {@link SQLQueryExpr} */
public class SQLExprParentSetterRule implements RewriteRule<SQLQueryExpr> {

  @Override
  public boolean match(SQLQueryExpr expr) {
    return true;
  }

  @Override
  public void rewrite(SQLQueryExpr expr) {
    expr.accept(new SQLExprParentSetter());
  }
}

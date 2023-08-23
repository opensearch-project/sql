/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.nestedfield;

import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import java.util.List;

/** Column list in SELECT statement. */
class Select extends SQLClause<List<SQLSelectItem>> {

  Select(List<SQLSelectItem> expr) {
    super(expr);
  }

  /**
   * Rewrite by adding nested field to SELECT in the case of 'SELECT *'.
   *
   * <p>Ex. 'SELECT *' => 'SELECT *, employees.*' So that NestedFieldProjection will add
   * 'employees.*' to includes list in inner_hits.
   */
  @Override
  void rewrite(Scope scope) {
    if (isSelectAllOnly()) {
      addSelectAllForNestedField(scope);
    }
  }

  private boolean isSelectAllOnly() {
    return expr.size() == 1 && expr.get(0).getExpr() instanceof SQLAllColumnExpr;
  }

  private void addSelectAllForNestedField(Scope scope) {
    for (String alias : scope.getAliases()) {
      expr.add(createSelectItem(alias + ".*"));
    }
  }

  private SQLSelectItem createSelectItem(String name) {
    return new SQLSelectItem(new SQLIdentifierExpr(name));
  }
}

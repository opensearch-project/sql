/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.parser;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;

/**
 * An Identifier that is wrapped in parentheses. This is for tracking in group bys the difference
 * between "group by state, age" and "group by (state), (age)". For non group by identifiers, it
 * acts as a normal SQLIdentifierExpr.
 */
public class SQLParensIdentifierExpr extends SQLIdentifierExpr {

  public SQLParensIdentifierExpr() {}

  public SQLParensIdentifierExpr(String name) {
    super(name);
  }

  public SQLParensIdentifierExpr(SQLIdentifierExpr expr) {
    super(expr.getName());
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.subquery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import org.junit.Test;
import org.opensearch.sql.legacy.rewriter.subquery.NestedQueryContext;

public class NestedQueryContextTest {

  @Test
  public void isNested() {
    NestedQueryContext nestedQueryDetector = new NestedQueryContext();
    nestedQueryDetector.add(new SQLExprTableSource(new SQLIdentifierExpr("employee"), "e"));

    assertFalse(
        nestedQueryDetector.isNested(new SQLExprTableSource(new SQLIdentifierExpr("e"), "e1")));
    assertTrue(
        nestedQueryDetector.isNested(
            new SQLExprTableSource(new SQLIdentifierExpr("e.projects"), "p")));

    nestedQueryDetector.add(new SQLExprTableSource(new SQLIdentifierExpr("e.projects"), "p"));
    assertTrue(nestedQueryDetector.isNested(new SQLExprTableSource(new SQLIdentifierExpr("p"))));
  }

  @Test
  public void isNestedJoin() {
    NestedQueryContext nestedQueryDetector = new NestedQueryContext();
    SQLJoinTableSource joinTableSource = new SQLJoinTableSource();
    joinTableSource.setLeft(new SQLExprTableSource(new SQLIdentifierExpr("employee"), "e"));
    joinTableSource.setRight(new SQLExprTableSource(new SQLIdentifierExpr("e.projects"), "p"));
    joinTableSource.setJoinType(JoinType.COMMA);
    nestedQueryDetector.add(joinTableSource);

    assertFalse(
        nestedQueryDetector.isNested(new SQLExprTableSource(new SQLIdentifierExpr("e"), "e1")));
    assertTrue(
        nestedQueryDetector.isNested(
            new SQLExprTableSource(new SQLIdentifierExpr("e.projects"), "p")));
    assertTrue(nestedQueryDetector.isNested(new SQLExprTableSource(new SQLIdentifierExpr("p"))));
  }

  @Test
  public void notNested() {
    NestedQueryContext nestedQueryDetector = new NestedQueryContext();
    nestedQueryDetector.add(new SQLExprTableSource(new SQLIdentifierExpr("employee"), "e"));
    nestedQueryDetector.add(new SQLExprTableSource(new SQLIdentifierExpr("projects"), "p"));

    assertFalse(
        nestedQueryDetector.isNested(new SQLExprTableSource(new SQLIdentifierExpr("e"), "e1")));
    assertFalse(nestedQueryDetector.isNested(new SQLExprTableSource(new SQLIdentifierExpr("p"))));
  }
}

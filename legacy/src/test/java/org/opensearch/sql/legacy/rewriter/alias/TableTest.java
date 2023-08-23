/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.alias;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import org.junit.Assert;
import org.junit.Test;

/** Test cases for util class {@link Table}. */
public class TableTest {

  @Test
  public void identifierOfTableNameShouldReturnTheTableName() {
    Table table = new Table(new SQLExprTableSource(new SQLIdentifierExpr("accounts")));
    Assert.assertEquals("accounts", table.name());
  }

  @Test
  public void identifierOfTableAndTypeNameShouldReturnTheTableNameOnly() {
    Table table =
        new Table(
            new SQLExprTableSource(
                new SQLBinaryOpExpr(
                    new SQLIdentifierExpr("accounts"),
                    SQLBinaryOperator.Divide,
                    new SQLIdentifierExpr("test"))));
    Assert.assertEquals("accounts", table.name());
  }
}

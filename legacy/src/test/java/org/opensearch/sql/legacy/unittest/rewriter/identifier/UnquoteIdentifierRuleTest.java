/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.identifier;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.rewriter.identifier.UnquoteIdentifierRule;
import org.opensearch.sql.legacy.util.SqlParserUtils;

/** Test cases for backticks quoted identifiers */
public class UnquoteIdentifierRuleTest {

  @Test
  public void queryWithQuotedIndex() {
    query("SELECT lastname FROM `bank` WHERE balance > 1000 ORDER BY age")
        .shouldBeAfterRewrite("SELECT lastname FROM bank WHERE balance > 1000 ORDER BY age");
  }

  @Test
  public void queryWithQuotedField() {
    query("SELECT `lastname` FROM bank ORDER BY age")
        .shouldBeAfterRewrite("SELECT lastname FROM bank ORDER BY age");

    query("SELECT b.`lastname` FROM bank AS b ORDER BY age")
        .shouldBeAfterRewrite("SELECT b.lastname FROM bank AS b ORDER BY age");
  }

  @Test
  public void queryWithQuotedAlias() {
    query("SELECT `b`.lastname FROM bank AS `b` ORDER BY age")
        .shouldBeAfterRewrite("SELECT b.lastname FROM bank AS b ORDER BY age");

    query("SELECT `b`.`lastname` FROM bank AS `b` ORDER BY age")
        .shouldBeAfterRewrite("SELECT b.lastname FROM bank AS b ORDER BY age");

    query("SELECT `b`.`lastname` AS `name` FROM bank AS `b` ORDER BY age")
        .shouldBeAfterRewrite("SELECT b.lastname AS name FROM bank AS b ORDER BY age");
  }

  @Test
  public void selectSpecificFieldsUsingQuotedTableNamePrefix() {
    query("SELECT `bank`.`lastname` FROM `bank`")
        .shouldBeAfterRewrite("SELECT bank.lastname FROM bank");
  }

  @Test
  public void queryWithQuotedAggrAndFunc() {
    query(
            ""
                + "SELECT `b`.`lastname` AS `name`, AVG(`b`.`balance`) FROM `bank` AS `b` "
                + "WHERE ABS(`b`.`age`) > 20 GROUP BY `b`.`lastname` ORDER BY `b`.`lastname`")
        .shouldBeAfterRewrite(
            "SELECT b.lastname AS name, AVG(b.balance) FROM bank AS b "
                + "WHERE ABS(b.age) > 20 GROUP BY b.lastname ORDER BY b.lastname");
  }

  private QueryAssertion query(String sql) {
    return new QueryAssertion(sql);
  }

  private static class QueryAssertion {

    private UnquoteIdentifierRule rule = new UnquoteIdentifierRule();
    private SQLQueryExpr expr;

    QueryAssertion(String sql) {
      this.expr = SqlParserUtils.parse(sql);
    }

    void shouldBeAfterRewrite(String expected) {
      rule.rewrite(expr);
      Assert.assertEquals(
          SQLUtils.toMySqlString(SqlParserUtils.parse(expected)), SQLUtils.toMySqlString(expr));
    }
  }
}

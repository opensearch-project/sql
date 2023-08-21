/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.ordinal;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.legacy.rewriter.matchtoterm.VerificationException;
import org.opensearch.sql.legacy.rewriter.ordinal.OrdinalRewriterRule;
import org.opensearch.sql.legacy.util.SqlParserUtils;

/** Test cases for ordinal aliases in GROUP BY and ORDER BY */
public class OrdinalRewriterRuleTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void ordinalInGroupByShouldMatch() {
    query("SELECT lastname FROM bank GROUP BY 1").shouldMatchRule();
  }

  @Test
  public void ordinalInOrderByShouldMatch() {
    query("SELECT lastname FROM bank ORDER BY 1").shouldMatchRule();
  }

  @Test
  public void ordinalInGroupAndOrderByShouldMatch() {
    query("SELECT lastname, age FROM bank GROUP BY 2, 1 ORDER BY 1").shouldMatchRule();
  }

  @Test
  public void noOrdinalInGroupByShouldNotMatch() {
    query("SELECT lastname FROM bank GROUP BY lastname").shouldNotMatchRule();
  }

  @Test
  public void noOrdinalInOrderByShouldNotMatch() {
    query("SELECT lastname, age FROM bank ORDER BY age").shouldNotMatchRule();
  }

  @Test
  public void noOrdinalInGroupAndOrderByShouldNotMatch() {
    query("SELECT lastname, age FROM bank GROUP BY lastname, age ORDER BY age")
        .shouldNotMatchRule();
  }

  @Test
  public void simpleGroupByOrdinal() {
    query("SELECT lastname FROM bank GROUP BY 1")
        .shouldBeAfterRewrite("SELECT lastname FROM bank GROUP BY lastname");
  }

  @Test
  public void multipleGroupByOrdinal() {
    query("SELECT lastname, age FROM bank GROUP BY 1, 2 ")
        .shouldBeAfterRewrite("SELECT lastname, age FROM bank GROUP BY lastname, age");

    query("SELECT lastname, age FROM bank GROUP BY 2, 1")
        .shouldBeAfterRewrite("SELECT lastname, age FROM bank GROUP BY age, lastname");

    query("SELECT lastname, age, firstname FROM bank GROUP BY 2, firstname, 1")
        .shouldBeAfterRewrite(
            "SELECT lastname, age, firstname FROM bank GROUP BY age, firstname, lastname");

    query("SELECT lastname, age, firstname FROM bank GROUP BY 2, something, 1")
        .shouldBeAfterRewrite(
            "SELECT lastname, age, firstname FROM bank GROUP BY age, something, lastname");
  }

  @Test
  public void simpleOrderByOrdinal() {
    query("SELECT lastname FROM bank ORDER BY 1")
        .shouldBeAfterRewrite("SELECT lastname FROM bank ORDER BY lastname");
  }

  @Test
  public void multipleOrderByOrdinal() {
    query("SELECT lastname, age FROM bank ORDER BY 1, 2 ")
        .shouldBeAfterRewrite("SELECT lastname, age FROM bank ORDER BY lastname, age");

    query("SELECT lastname, age FROM bank ORDER BY 2, 1")
        .shouldBeAfterRewrite("SELECT lastname, age FROM bank ORDER BY age, lastname");

    query("SELECT lastname, age, firstname FROM bank ORDER BY 2, firstname, 1")
        .shouldBeAfterRewrite(
            "SELECT lastname, age, firstname FROM bank ORDER BY age, firstname, lastname");

    query("SELECT lastname, age, firstname FROM bank ORDER BY 2, department, 1")
        .shouldBeAfterRewrite(
            "SELECT lastname, age, firstname FROM bank ORDER BY age, department, lastname");
  }

  // Tests invalid Ordinals, non-positive ordinal values are already validated by semantic analyzer
  @Test
  public void invalidGroupByOrdinalShouldThrowException() {
    exception.expect(VerificationException.class);
    exception.expectMessage("Invalid ordinal [3] specified in [GROUP BY 3]");
    query("SELECT lastname, MAX(lastname) FROM bank GROUP BY 3 ").rewrite();
  }

  @Test
  public void invalidOrderByOrdinalShouldThrowException() {
    exception.expect(VerificationException.class);
    exception.expectMessage("Invalid ordinal [4] specified in [ORDER BY 4]");
    query("SELECT `lastname`, `age`, `firstname` FROM bank ORDER BY 4 IS NOT NULL").rewrite();
  }

  private QueryAssertion query(String sql) {
    return new QueryAssertion(sql);
  }

  private static class QueryAssertion {

    private OrdinalRewriterRule rule;
    private SQLQueryExpr expr;

    QueryAssertion(String sql) {
      this.expr = SqlParserUtils.parse(sql);
      this.rule = new OrdinalRewriterRule(sql);
    }

    void shouldBeAfterRewrite(String expected) {
      shouldMatchRule();
      rule.rewrite(expr);
      Assert.assertEquals(
          SQLUtils.toMySqlString(SqlParserUtils.parse(expected)), SQLUtils.toMySqlString(expr));
    }

    void shouldMatchRule() {
      Assert.assertTrue(match());
    }

    void shouldNotMatchRule() {
      Assert.assertFalse(match());
    }

    void rewrite() {
      shouldMatchRule();
      rule.rewrite(expr);
    }

    private boolean match() {
      return rule.match(expr);
    }
  }
}

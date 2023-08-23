/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.alias;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.util.SqlParserUtils;

/** Test cases for field name prefix remove rule. */
public class TableAliasPrefixRemoveRuleTest {

  @Test
  public void queryWithUnAliasedTableNameShouldMatch() {
    query("SELECT account.age FROM accounts").shouldMatchRule();
  }

  @Test
  public void queryWithUnAliasedTableNameInSubQueryShouldNotMatch() {
    query("SELECT * FROM test t WHERE t.name IN (SELECT accounts.name FROM accounts)")
        .shouldNotMatchRule();
  }

  @Test
  public void queryWithoutUnAliasedTableNameShouldMatch() {
    query("SELECT a.age FROM accounts a WHERE a.balance > 1000").shouldMatchRule();
  }

  @Test
  public void joinQueryWithoutUnAliasedTableNameShouldNotMatch() {
    query("SELECT * FROM accounts a1 JOIN accounts a2 ON a1.city = a2.city").shouldNotMatchRule();
  }

  @Test
  public void nestedFieldQueryWithoutUnAliasedTableNameShouldNotMatch() {
    query("SELECT * FROM accounts a, a.project p").shouldNotMatchRule();
  }

  @Test
  public void selectedFieldNamePrefixedByUnAliasedTableNameShouldRemoveTableNamePrefix() {
    query("SELECT accounts.age FROM accounts").shouldBeAfterRewrite("SELECT age FROM accounts");
    query("SELECT accounts.age FROM accounts/temp")
        .shouldBeAfterRewrite("SELECT age FROM accounts/temp");
    query("SELECT age FROM accounts/temp a").shouldBeAfterRewrite("SELECT age FROM accounts/temp");
  }

  @Test
  public void allFieldNamePrefixedByUnAliasedTableNameEverywhereShouldRemoveTableNamePrefix() {
    query(
            "SELECT accounts.age, AVG(accounts.salary) FROM accounts WHERE accounts.age > 10 "
                + "GROUP BY accounts.age HAVING AVG(accounts.balance) > 1000 ORDER BY accounts.age")
        .shouldBeAfterRewrite(
            "SELECT age, AVG(salary) FROM accounts WHERE age > 10 "
                + "GROUP BY age HAVING AVG(balance) > 1000 ORDER BY age");
  }

  @Test
  public void selectedFieldNamePrefixedByTableAliasShouldRemoveTableAliasPrefix() {
    query("SELECT a.age FROM accounts a").shouldBeAfterRewrite("SELECT age FROM accounts");
    query("SELECT a.age FROM accounts/temp a")
        .shouldBeAfterRewrite("SELECT age FROM accounts/temp");
  }

  @Test
  public void allFieldNamePrefixedByTableAliasShouldRemoveTableAliasPrefix() {
    query(
            "SELECT a.age, AVG(a.salary) FROM accounts a WHERE a.age > 10 "
                + "GROUP BY a.age HAVING AVG(a.balance) > 1000 ORDER BY a.age")
        .shouldBeAfterRewrite(
            "SELECT age, AVG(salary) FROM accounts WHERE age > 10 "
                + "GROUP BY age HAVING AVG(balance) > 1000 ORDER BY age");
  }

  @Test
  public void allFieldNamePrefixedByTableAliasInMultiQueryShouldRemoveTableAliasPrefix() {
    query("SELECT t.name FROM test t UNION SELECT a.age FROM accounts a WHERE a.age > 10")
        .shouldBeAfterRewrite(
            "SELECT name FROM test UNION SELECT age FROM accounts WHERE age > 10");
  }

  @Test
  public void unAliasedFieldNameShouldNotBeChanged() {
    query("SELECT a.age, name FROM accounts a WHERE balance > 1000")
        .shouldBeAfterRewrite("SELECT age, name FROM accounts WHERE balance > 1000");
    query("SELECT accounts.age, name FROM accounts WHERE balance > 1000")
        .shouldBeAfterRewrite("SELECT age, name FROM accounts WHERE balance > 1000");
  }

  private QueryAssertion query(String sql) {
    return new QueryAssertion(sql);
  }

  private static class QueryAssertion {

    private final TableAliasPrefixRemoveRule rule = new TableAliasPrefixRemoveRule();

    private final SQLQueryExpr expr;

    QueryAssertion(String sql) {
      this.expr = SqlParserUtils.parse(sql);
    }

    void shouldMatchRule() {
      Assert.assertTrue(match());
    }

    void shouldNotMatchRule() {
      Assert.assertFalse(match());
    }

    void shouldBeAfterRewrite(String expected) {
      shouldMatchRule();
      rule.rewrite(expr);
      Assert.assertEquals(
          SQLUtils.toMySqlString(SqlParserUtils.parse(expected)), SQLUtils.toMySqlString(expr));
    }

    private boolean match() {
      return rule.match(expr);
    }
  }
}

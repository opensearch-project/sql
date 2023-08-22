/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.parser;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.util.TestsConstants.TEST_INDEX_ACCOUNT;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import org.junit.Test;
import org.opensearch.sql.legacy.domain.Condition;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.parser.ElasticSqlExprParser;
import org.opensearch.sql.legacy.parser.SqlParser;
import org.opensearch.sql.legacy.utils.StringUtils;

public class SubQueryParserTest {

  private static SqlParser parser = new SqlParser();

  @Test
  public void selectFromSubqueryShouldPass() throws SqlParseException {
    Select select =
        parseSelect(
            StringUtils.format(
                "SELECT t.T1 as age1, t.T2 as balance1 "
                    + "FROM (SELECT age as T1, balance as T2 FROM %s/account) t",
                TEST_INDEX_ACCOUNT));

    assertEquals(2, select.getFields().size());
    assertEquals("age", select.getFields().get(0).getName());
    assertEquals("age1", select.getFields().get(0).getAlias());
    assertEquals("balance", select.getFields().get(1).getName());
    assertEquals("balance1", select.getFields().get(1).getAlias());
  }

  @Test
  public void selectFromSubqueryWithoutAliasShouldPass() throws SqlParseException {
    Select select =
        parseSelect(
            StringUtils.format(
                "SELECT t.age as finalAge, t.balance as finalBalance "
                    + "FROM (SELECT age, balance FROM %s/account) t",
                TEST_INDEX_ACCOUNT));

    assertEquals(2, select.getFields().size());
    assertEquals("age", select.getFields().get(0).getName());
    assertEquals("finalAge", select.getFields().get(0).getAlias());
    assertEquals("balance", select.getFields().get(1).getName());
    assertEquals("finalBalance", select.getFields().get(1).getAlias());
  }

  @Test
  public void selectFromSubqueryShouldIgnoreUnusedField() throws SqlParseException {
    Select select =
        parseSelect(
            StringUtils.format(
                "SELECT t.T1 as age1 " + "FROM (SELECT age as T1, balance as T2 FROM %s/account) t",
                TEST_INDEX_ACCOUNT));

    assertEquals(1, select.getFields().size());
    assertEquals("age", select.getFields().get(0).getName());
    assertEquals("age1", select.getFields().get(0).getAlias());
  }

  @Test
  public void selectFromSubqueryWithAggShouldPass() throws SqlParseException {
    Select select =
        parseSelect(
            StringUtils.format(
                "SELECT t.TEMP as count " + "FROM (SELECT COUNT(*) as TEMP FROM %s/account) t",
                TEST_INDEX_ACCOUNT));
    assertEquals(1, select.getFields().size());
    assertEquals("COUNT", select.getFields().get(0).getName());
    assertEquals("count", select.getFields().get(0).getAlias());
  }

  @Test
  public void selectFromSubqueryWithWhereAndCountShouldPass() throws SqlParseException {
    Select select =
        parseSelect(
            StringUtils.format(
                "SELECT t.TEMP as count "
                    + "FROM (SELECT COUNT(*) as TEMP FROM %s/account WHERE age > 30) t",
                TEST_INDEX_ACCOUNT));

    assertEquals(1, select.getFields().size());
    assertEquals("COUNT", select.getFields().get(0).getName());
    assertEquals("count", select.getFields().get(0).getAlias());
  }

  @Test
  public void selectFromSubqueryWithCountAndGroupByAndOrderByShouldPass() throws SqlParseException {
    Select select =
        parseSelect(
            StringUtils.format(
                "SELECT t.TEMP as count "
                    + "FROM (SELECT COUNT(*) as TEMP FROM %s/account GROUP BY age ORDER BY TEMP) t",
                TEST_INDEX_ACCOUNT));

    assertEquals(1, select.getFields().size());
    assertEquals("COUNT", select.getFields().get(0).getName());
    assertEquals("count", select.getFields().get(0).getAlias());
    assertEquals(1, select.getOrderBys().size());
    assertEquals("count", select.getOrderBys().get(0).getName());
    assertEquals("count", select.getOrderBys().get(0).getSortField().getName());
  }

  @Test
  public void selectFromSubqueryWithCountAndGroupByAndHavingShouldPass() throws Exception {

    Select select =
        parseSelect(
            StringUtils.format(
                "SELECT t.T1 as g, t.T2 as c "
                    + "FROM (SELECT gender as T1, COUNT(*) as T2 "
                    + "      FROM %s/account "
                    + "      GROUP BY gender "
                    + "      HAVING T2 > 500) t",
                TEST_INDEX_ACCOUNT));

    assertEquals(2, select.getFields().size());
    assertEquals("gender", select.getFields().get(0).getName());
    assertEquals("g", select.getFields().get(0).getAlias());
    assertEquals("COUNT", select.getFields().get(1).getName());
    assertEquals("c", select.getFields().get(1).getAlias());
    assertEquals(1, select.getHaving().getConditions().size());
    assertEquals("c", ((Condition) select.getHaving().getConditions().get(0)).getName());
  }

  @Test
  public void selectFromSubqueryCountAndSum() throws Exception {
    Select select =
        parseSelect(
            StringUtils.format(
                "SELECT t.TEMP1 as count, t.TEMP2 as balance "
                    + "FROM (SELECT COUNT(*) as TEMP1, SUM(balance) as TEMP2 "
                    + "      FROM %s/account) t",
                TEST_INDEX_ACCOUNT));
    assertEquals(2, select.getFields().size());
    assertEquals("COUNT", select.getFields().get(0).getName());
    assertEquals("count", select.getFields().get(0).getAlias());
    assertEquals("SUM", select.getFields().get(1).getName());
    assertEquals("balance", select.getFields().get(1).getAlias());
  }

  private Select parseSelect(String query) throws SqlParseException {
    return parser.parseSelect((SQLQueryExpr) new ElasticSqlExprParser(query).expr());
  }
}

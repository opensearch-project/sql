/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import org.apache.calcite.sql.parser.SqlParserFixture;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.executor.QueryType;

/**
 * SQL parser tests using Calcite's {@link SqlParserFixture} for idiomatic parse-unparse assertions.
 * Parser config is read from {@link org.opensearch.sql.api.UnifiedQueryContext} to stay in sync
 * with production.
 */
public class UnifiedQueryParserSqlTest extends UnifiedQueryTestBase {

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Test
  public void testParseSelectStar() {
    sql("SELECT * FROM catalog.employees")
        .ok(
            """
            SELECT *
            FROM `catalog`.`employees`\
            """);
  }

  @Test
  public void testParseSelectColumns() {
    sql("SELECT id, name FROM catalog.employees")
        .ok(
            """
            SELECT `id`, `name`
            FROM `catalog`.`employees`\
            """);
  }

  @Test
  public void testParseFilter() {
    sql("""
    SELECT name
    FROM catalog.employees
    WHERE age > 30\
    """)
        .ok(
            """
            SELECT `name`
            FROM `catalog`.`employees`
            WHERE (`age` > 30)\
            """);
  }

  @Test
  public void testParseAggregate() {
    sql("""
    SELECT department, count(*) AS cnt
    FROM catalog.employees
    GROUP BY department\
    """)
        .ok(
            """
            SELECT `department`, COUNT(*) AS `cnt`
            FROM `catalog`.`employees`
            GROUP BY `department`\
            """);
  }

  @Test
  public void testParseOrderBy() {
    sql("""
    SELECT name
    FROM catalog.employees
    ORDER BY age DESC\
    """)
        .ok(
            """
            SELECT `name`
            FROM `catalog`.`employees`
            ORDER BY `age` DESC\
            """);
  }

  @Test
  public void testParseJoin() {
    sql("""
    SELECT a.id, b.name
    FROM catalog.employees a
    JOIN catalog.employees b ON a.id = b.age\
    """)
        .ok(
            """
            SELECT `a`.`id`, `b`.`name`
            FROM `catalog`.`employees` AS `a`
            INNER JOIN `catalog`.`employees` AS `b` ON (`a`.`id` = `b`.`age`)\
            """);
  }

  @Test
  public void testSyntaxErrorFails() {
    sql("SELECT ^FROM^").fails("(?s).*Incorrect syntax near the keyword 'FROM'.*");
  }

  private SqlParserFixture sql(String sql) {
    return SqlParserFixture.DEFAULT
        .withConfig(c -> context.getPlanContext().config.getParserConfig())
        .sql(sql);
  }
}

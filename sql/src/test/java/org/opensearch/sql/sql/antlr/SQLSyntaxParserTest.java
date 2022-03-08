/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.antlr;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

class SQLSyntaxParserTest {

  private final SQLSyntaxParser parser = new SQLSyntaxParser();

  @Test
  public void canParseQueryEndWithSemiColon() {
    assertNotNull(parser.parse("SELECT 123;"));
  }

  @Test
  public void canParseSelectLiterals() {
    assertNotNull(parser.parse("SELECT 123, 'hello'"));
  }

  @Test
  public void canParseSelectLiteralWithAlias() {
    assertNotNull(parser.parse("SELECT (1 + 2) * 3 AS expr"));
  }

  @Test
  public void canParseSelectFields() {
    assertNotNull(parser.parse("SELECT name, age FROM accounts"));
  }

  @Test
  public void canParseSelectFieldWithAlias() {
    assertNotNull(parser.parse("SELECT name AS n, age AS a FROM accounts"));
  }

  @Test
  public void canParseSelectFieldWithQuotedAlias() {
    assertNotNull(parser.parse("SELECT name AS `n` FROM accounts"));
  }

  @Test
  public void canParseIndexNameWithDate() {
    assertNotNull(parser.parse("SELECT * FROM logs_2020_01"));
    assertNotNull(parser.parse("SELECT * FROM logs-2020-01"));
  }

  @Test
  public void canParseHiddenIndexName() {
    assertNotNull(parser.parse("SELECT * FROM .opensearch_dashboards"));
  }

  @Test
  public void canNotParseIndexNameWithSpecialChar() {
    assertThrows(SyntaxCheckException.class,
        () -> parser.parse("SELECT * FROM hello+world"));
  }

  @Test
  public void canParseIndexNameWithSpecialCharQuoted() {
    assertNotNull(parser.parse("SELECT * FROM `hello+world`"));
  }

  @Test
  public void canNotParseIndexNameStartingWithNumber() {
    assertThrows(SyntaxCheckException.class,
        () -> parser.parse("SELECT * FROM 123test"));
  }

  @Test
  public void canNotParseIndexNameSingleQuoted() {
    assertThrows(SyntaxCheckException.class,
        () -> parser.parse("SELECT * FROM 'test'"));
  }

  @Test
  public void canParseWhereClause() {
    assertNotNull(parser.parse("SELECT name FROM test WHERE age = 10"));
  }

  @Test
  public void canParseSelectClauseWithLogicalOperator() {
    assertNotNull(parser.parse(
        "SELECT age = 10 AND name = 'John' OR NOT (balance > 1000) FROM test"));
  }

  @Test
  public void canParseWhereClauseWithLogicalOperator() {
    assertNotNull(parser.parse("SELECT name FROM test "
        + "WHERE age = 10 AND name = 'John' OR NOT (balance > 1000)"));
  }

  @Test
  public void canParseGroupByClause() {
    assertNotNull(parser.parse("SELECT name, AVG(age) FROM test GROUP BY name"));
    assertNotNull(parser.parse("SELECT name AS n, AVG(age) FROM test GROUP BY n"));
    assertNotNull(parser.parse("SELECT ABS(balance) FROM test GROUP BY ABS(balance)"));
    assertNotNull(parser.parse("SELECT ABS(balance) FROM test GROUP BY 1"));
  }

  @Test
  public void canParseDistinctClause() {
    assertNotNull(parser.parse("SELECT DISTINCT name FROM test"));
    assertNotNull(parser.parse("SELECT DISTINCT name, balance FROM test"));
  }

  @Test
  public void canParseCaseStatement() {
    assertNotNull(parser.parse("SELECT CASE WHEN age > 30 THEN 'age1' ELSE 'age2' END FROM test"));
    assertNotNull(parser.parse("SELECT CASE WHEN age > 30 THEN 'age1' "
                                        + " WHEN age < 50 THEN 'age2' "
                                        + " ELSE 'age3' END FROM test"));
    assertNotNull(parser.parse("SELECT CASE age WHEN 30 THEN 'age1' ELSE 'age2' END FROM test"));
    assertNotNull(parser.parse("SELECT CASE age WHEN 30 THEN 'age1' END FROM test"));
  }

  @Test
  public void canNotParseAggregateFunctionWithWrongArgument() {
    assertThrows(SyntaxCheckException.class, () -> parser.parse("SELECT SUM() FROM test"));
    assertThrows(SyntaxCheckException.class, () -> parser.parse("SELECT AVG() FROM test"));
    assertThrows(SyntaxCheckException.class, () -> parser.parse("SELECT SUM(a,b) FROM test"));
    assertThrows(SyntaxCheckException.class, () -> parser.parse("SELECT AVG(a,b,c) FROM test"));
  }

  @Test
  public void canParseOrderByClause() {
    assertNotNull(parser.parse("SELECT name, age FROM test ORDER BY name, age"));
    assertNotNull(parser.parse("SELECT name, age FROM test ORDER BY name ASC, age DESC"));
    assertNotNull(parser.parse(
        "SELECT name, age FROM test ORDER BY name NULLS LAST, age NULLS FIRST"));
    assertNotNull(parser.parse(
        "SELECT name, age FROM test ORDER BY name ASC NULLS FIRST, age DESC NULLS LAST"));
  }

  @Test
  public void canNotParseShowStatementWithoutFilterClause() {
    assertThrows(SyntaxCheckException.class, () -> parser.parse("SHOW TABLES"));
  }

}

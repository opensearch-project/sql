/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.antlr;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  @Test
  public void canParseRelevanceFunctions() {
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(column, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(column, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(`column`, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(`column`, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(column, 100500)"));

    assertNotNull(parser.parse("SELECT * FROM test WHERE match_phrase(column, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_phrase(column, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_phrase(`column`, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_phrase(`column`, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_phrase(column, 100500)"));
  }

  @ParameterizedTest
  @MethodSource("matchPhraseComplexQueries")
  public void canParseComplexMatchPhraseArgsTest(String query) {
    assertNotNull(parser.parse(query));
  }

  private static Stream<String> matchPhraseComplexQueries() {
    return Stream.of(
      "SELECT * FROM t WHERE match_phrase(c, 3)",
      "SELECT * FROM t WHERE match_phrase(c, 3, fuzziness=AUTO)",
      "SELECT * FROM t WHERE match_phrase(c, 3, zero_terms_query=\"all\")",
      "SELECT * FROM t WHERE match_phrase(c, 3, lenient=true)",
      "SELECT * FROM t WHERE match_phrase(c, 3, lenient='true')",
      "SELECT * FROM t WHERE match_phrase(c, 3, operator=xor)",
      "SELECT * FROM t WHERE match_phrase(c, 3, cutoff_frequency=0.04)",
      "SELECT * FROM t WHERE match_phrase(c, 3, cutoff_frequency=0.04, analyzer = english, prefix_length=34, fuzziness='auto', minimum_should_match='2<-25% 9<-3')",
      "SELECT * FROM t WHERE match_phrase(c, 3, minimum_should_match='2<-25% 9<-3')",
      "SELECT * FROM t WHERE match_phrase(c, 3, operator='AUTO')"

    );
  }

  private void generateAndTestQuery(String function, HashMap<String, Object[]> functionArgs) {
    var rand = new Random(0);

    for (int i = 0; i < 100; i++)
    {
      StringBuilder query = new StringBuilder();
      query.append(String.format("SELECT * FROM test WHERE %s(%s, %s", function,
              RandomStringUtils.random(10, true, false),
              RandomStringUtils.random(10, true, false)));
      var args = new ArrayList<String>();
      for (var pair : functionArgs.entrySet())
      {
        if (rand.nextBoolean())
        {
          var arg = new StringBuilder();
          arg.append(rand.nextBoolean() ? "," : ", ");
          arg.append(rand.nextBoolean() ? pair.getKey().toLowerCase() : pair.getKey().toUpperCase());
          arg.append(rand.nextBoolean() ? "=" : " = ");
          if (pair.getValue() instanceof String[] || rand.nextBoolean()) {
            var quoteSymbol = rand.nextBoolean() ? '\'' : '"';
            arg.append(quoteSymbol);
            arg.append(pair.getValue()[rand.nextInt(pair.getValue().length)]);
            arg.append(quoteSymbol);
          }
          else
            arg.append(pair.getValue()[rand.nextInt(pair.getValue().length)]);
          args.add(arg.toString());
        }
      }
      Collections.shuffle(args);
      for (var arg : args)
        query.append(arg);
      query.append(rand.nextBoolean() ? ")" : ");");
      //System.out.printf("%d, %s%n", i, query.toString());
      assertNotNull(parser.parse(query.toString()));
    }
  }

  // TODO run all tests and collect exceptions and raise them in the end
  @Test
  public void canParseRelevanceFunctionsComplexRandomArgs() {
    var matchArgs = new HashMap<String, Object[]>();
    matchArgs.put("fuzziness", new String[]{ "AUTO", "AUTO:1,5", "1" });
    matchArgs.put("fuzzy_transpositions", new Boolean[]{ true, false });
    matchArgs.put("operator", new String[]{ "and", "or" });
    matchArgs.put("minimum_should_match", new String[]{ "3", "-2", "75%", "-25%", "3<90%", "2<-25% 9<-3" });
    matchArgs.put("analyzer", new String[]{ "standard", "stop", "english" });
    matchArgs.put("zero_terms_query", new String[]{ "none", "all" });
    matchArgs.put("lenient", new Boolean[]{ true, false });
    // deprecated
    matchArgs.put("cutoff_frequency", new Double[]{ .0, 0.001, 1., 42. });
    matchArgs.put("prefix_length", new Integer[]{ 0, 2, 5 });
    matchArgs.put("max_expansions", new Integer[]{ 0, 5, 20 });
    matchArgs.put("boost", new Double[]{ .5, 1., 2.3 });

    generateAndTestQuery("match", matchArgs);

    var matchPhraseArgs = new HashMap<String, Object[]>();
    matchPhraseArgs.put("analyzer", new String[]{ "standard", "stop", "english" });
    matchPhraseArgs.put("max_expansions", new Integer[]{ 0, 5, 20 });
    matchPhraseArgs.put("slop", new Integer[]{ 0, 1, 2 });

    generateAndTestQuery("match_phrase", matchPhraseArgs);
  }
}

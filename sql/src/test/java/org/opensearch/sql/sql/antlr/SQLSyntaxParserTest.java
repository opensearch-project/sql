/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.antlr;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

  private static Stream<Arguments> nowLikeFunctionsData() {
    return Stream.of(
        Arguments.of("now", true, false),
        Arguments.of("current_timestamp", true, true),
        Arguments.of("localtimestamp", true, true),
        Arguments.of("localtime", true, true),
        Arguments.of("sysdate", true, false),
        Arguments.of("curtime", true, false),
        Arguments.of("current_time", true, true),
        Arguments.of("curdate", false, false),
        Arguments.of("current_date", false, true),
        Arguments.of("utc_date", false, true),
        Arguments.of("utc_time", false, true),
        Arguments.of("utc_timestamp", false, true)
    );
  }

  private static Stream<Arguments> getPartForExtractFunction() {
    return Stream.of(
        Arguments.of("MICROSECOND"),
        Arguments.of("SECOND"),
        Arguments.of("MINUTE"),
        Arguments.of("HOUR"),
        Arguments.of("DAY"),
        Arguments.of("WEEK"),
        Arguments.of("MONTH"),
        Arguments.of("QUARTER"),
        Arguments.of("YEAR"),
        Arguments.of("SECOND_MICROSECOND"),
        Arguments.of("MINUTE_MICROSECOND"),
        Arguments.of("MINUTE_SECOND"),
        Arguments.of("HOUR_MICROSECOND"),
        Arguments.of("HOUR_SECOND"),
        Arguments.of("HOUR_MINUTE"),
        Arguments.of("DAY_MICROSECOND"),
        Arguments.of("DAY_SECOND"),
        Arguments.of("DAY_MINUTE"),
        Arguments.of("DAY_HOUR"),
        Arguments.of("YEAR_MONTH")
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getPartForExtractFunction")
  public void can_parse_extract_function(String part) {
    assertNotNull(parser.parse(String.format("SELECT extract(%s FROM \"2023-02-06\")", part)));
  }

  private static Stream<Arguments> getInvalidPartForExtractFunction() {
    return Stream.of(
        Arguments.of("INVALID"),
        Arguments.of("\"SECOND\""),
        Arguments.of("123")
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getInvalidPartForExtractFunction")
  public void cannot_parse_extract_function_invalid_part(String part) {
    assertThrows(
        SyntaxCheckException.class,
        () -> parser.parse(String.format("SELECT extract(%s FROM \"2023-02-06\")", part)));
  }

  @Test
  public void can_parse_weekday_function() {
    assertNotNull(parser.parse("SELECT weekday('2022-11-18')"));
    assertNotNull(parser.parse("SELECT day_of_week('2022-11-18')"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("nowLikeFunctionsData")
  public void can_parse_now_like_functions(String name, Boolean hasFsp, Boolean hasShortcut) {
    var calls = new ArrayList<String>() {{
        add(name + "()");
      }};
    if (hasShortcut) {
      calls.add(name);
    }
    if (hasFsp) {
      calls.add(name + "(0)");
    }

    assertNotNull(parser.parse("SELECT " + String.join(", ", calls)));
    assertNotNull(parser.parse("SELECT " + String.join(", ", calls) + " FROM test"));
    assertNotNull(parser.parse("SELECT " + String.join(", ", calls) + ", id FROM test"));
    assertNotNull(parser.parse("SELECT id FROM test WHERE " + String.join(" AND ", calls)));
  }

  private static Stream<Arguments> get_format_arguments() {
    Stream.Builder<Arguments> args = Stream.builder();
    String[] types = {"DATE", "DATETIME", "TIME", "TIMESTAMP"};
    String[] formats = {"'USA'", "'JIS'", "'ISO'", "'EUR'", "'INTERNAL'"};

    for (String type : types) {
      for (String format : formats) {
        args.add(Arguments.of(type, format));
      }
    }

    return args.build();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("get_format_arguments")
  public void can_parse_get_format_function(String type, String format) {
    assertNotNull(parser.parse(String.format("SELECT GET_FORMAT(%s, %s)", type, format)));
  }

  @Test
  public void cannot_parse_get_format_function_with_bad_arg() {
    assertThrows(
        SyntaxCheckException.class,
        () -> parser.parse("GET_FORMAT(NONSENSE_ARG,'INTERNAL')"));
  }

  @Test
  public void can_parse_hour_functions() {
    assertNotNull(parser.parse("SELECT hour('2022-11-18 12:23:34')"));
    assertNotNull(parser.parse("SELECT hour_of_day('12:23:34')"));
  }

  @Test
  public void can_parse_week_of_year_functions() {
    assertNotNull(parser.parse("SELECT week('2022-11-18')"));
    assertNotNull(parser.parse("SELECT week_of_year('2022-11-18')"));
  }

  @Test
  public void can_parse_dayofmonth_functions() {
    assertNotNull(parser.parse("SELECT dayofmonth('2022-11-18')"));
    assertNotNull(parser.parse("SELECT day_of_month('2022-11-18')"));
  }

  @Test
  public void can_parse_day_of_week_functions() {
    assertNotNull(parser.parse("SELECT dayofweek('2022-11-18')"));
    assertNotNull(parser.parse("SELECT day_of_week('2022-11-18')"));
  }

  @Test
  public void can_parse_dayofyear_functions() {
    assertNotNull(parser.parse("SELECT dayofyear('2022-11-18')"));
    assertNotNull(parser.parse("SELECT day_of_year('2022-11-18')"));
  }

  @Test
  public void can_parse_minute_functions() {
    assertNotNull(parser.parse("SELECT minute('12:23:34')"));
    assertNotNull(parser.parse("SELECT minute_of_hour('12:23:34')"));

    assertNotNull(parser.parse("SELECT minute('2022-12-20 12:23:34')"));
    assertNotNull(parser.parse("SELECT minute_of_hour('2022-12-20 12:23:34')"));
  }

  @Test
  public void can_parse_month_of_year_function() {
    assertNotNull(parser.parse("SELECT month('2022-11-18')"));
    assertNotNull(parser.parse("SELECT month_of_year('2022-11-18')"));

    assertNotNull(parser.parse("SELECT month(date('2022-11-18'))"));
    assertNotNull(parser.parse("SELECT month_of_year(date('2022-11-18'))"));

    assertNotNull(parser.parse("SELECT month(datetime('2022-11-18 00:00:00'))"));
    assertNotNull(parser.parse("SELECT month_of_year(datetime('2022-11-18 00:00:00'))"));

    assertNotNull(parser.parse("SELECT month(timestamp('2022-11-18 00:00:00'))"));
    assertNotNull(parser.parse("SELECT month_of_year(timestamp('2022-11-18 00:00:00'))"));

  }

  @Test
  public void can_parse_multi_match_relevance_function() {
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multimatch(\"fields\"=\"field\", query=\"query\")"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multimatchquery(fields=\"field\", \"query\"=\"query\")"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(\"fields\"=\"field\", \"query\"=\"query\")"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(\'fields\'=\'field\', \'query\'=\'query\')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(fields=\'field\', query=\'query\')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(['address'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(['address', 'notes'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match([\"*\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match([\"address\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match([`address`], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match([address], 'query')"));

    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " multi_match(['address' ^ 1.0, 'notes' ^ 2.2], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(['address', 'notes' ^ 1.5], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(['address', 'notes' 3], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE multi_match(['address' ^ .3, 'notes' 3], 'query')"));

    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " multi_match([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " multi_match([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query', analyzer=keyword,"
            + "operator='AND', tie_breaker=0.3, type = \"most_fields\", fuzziness = \"AUTO\")"));
  }

  @Test
  public void can_parse_second_functions() {
    assertNotNull(parser.parse("SELECT second('12:23:34')"));
    assertNotNull(parser.parse("SELECT second_of_minute('2022-11-18')"));
    assertNotNull(parser.parse("SELECT second('2022-11-18 12:23:34')"));
    assertNotNull(parser.parse("SELECT second_of_minute('2022-11-18 12:23:34')"));
  }

  @Test
  public void can_parse_simple_query_string_relevance_function() {
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string(['address'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string(['address', 'notes'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string([\"*\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string([\"address\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string([`address`], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string([address], 'query')"));

    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " simple_query_string(['address' ^ 1.0, 'notes' ^ 2.2], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string(['address', 'notes' ^ 1.5], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string(['address', 'notes' 3], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE simple_query_string(['address' ^ .3, 'notes' 3], 'query')"));

    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " simple_query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " simple_query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query', analyzer=keyword,"
            + "flags='AND', quote_field_suffix=\".exact\", fuzzy_prefix_length = 4)"));
  }

  @Test
  public void can_parse_str_to_date() {
    assertNotNull(parser.parse(
        "SELECT STR_TO_DATE('01,5,2013','%d,%m,%Y')"
    ));

    assertNotNull(parser.parse(
        "SELECT STR_TO_DATE('a09:30:17','a%h:%i:%s')"
    ));

    assertNotNull(parser.parse(
        "SELECT STR_TO_DATE('abc','abc');"
    ));
  }

  @Test
  public void can_parse_query_string_relevance_function() {
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['*'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['address'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['add*'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['*ess'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['address', 'notes'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([\"*\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([\"address\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([\"ad*\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([\"*s\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([\"address\", \"notes\"], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([`*`], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([`address`], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([`ad*`], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([`*ss`], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([`address`, `notes`], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([address], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([addr*], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([*ss], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string([address, notes], 'query')"));

    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " query_string(['address' ^ 1.0, 'notes' ^ 2.2], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['address', 'notes' ^ 1.5], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['address', 'notes' 3], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE query_string(['address' ^ .3, 'notes' 3], 'query')"));

    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query')"));
    assertNotNull(parser.parse(
        "SELECT id FROM test WHERE"
            + " query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query', analyzer=keyword,"
            + "operator='AND', tie_breaker=0.3, type = \"most_fields\", fuzziness = 4)"));
  }


  @Test
  public void can_parse_query_relevance_function() {
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query('address:query')"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query('address:query OR notes:query')"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(\"address:query\")"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(\"address:query OR notes:query\")"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(`address:query`)"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(`address:query OR notes:query`)"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query('*:query')"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(\"*:query\")"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(`*:query`)"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query('address:*uery OR notes:?uery')"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(\"address:*uery OR notes:?uery\")"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(`address:*uery OR notes:?uery`)"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query('address:qu*ry OR notes:qu?ry')"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(\"address:qu*ry OR notes:qu?ry\")"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(`address:qu*ry OR notes:qu?ry`)"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query('address:query notes:query')"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE query(\"address:query notes:query\")"));
    assertNotNull(parser.parse(
            "SELECT id FROM test WHERE "
                    + "query(\"Body:\'taste beer\' Tags:\'taste beer\'  Title:\'taste beer\'\")"));
  }


  @Test
  public void can_parse_match_relevance_function() {
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(column, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(column, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(`column`, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(`column`, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match(column, 100500)"));
  }

  @Test
  public void can_parse_matchquery_relevance_function() {
    assertNotNull(parser.parse("SELECT * FROM test WHERE matchquery(column, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE matchquery(column, 'this is a test')"));
    assertNotNull(parser.parse(
        "SELECT * FROM test WHERE matchquery(`column`, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE matchquery(`column`, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE matchquery(column, 100500)"));
  }

  @Test
  public void can_parse_match_query_relevance_function() {
    assertNotNull(parser.parse(
        "SELECT * FROM test WHERE match_query(column, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_query(column, 'this is a test')"));
    assertNotNull(parser.parse(
        "SELECT * FROM test WHERE match_query(`column`, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_query(`column`, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_query(column, 100500)"));
  }

  @Test
  public void can_parse_match_phrase_relevance_function() {
    assertNotNull(
            parser.parse("SELECT * FROM test WHERE match_phrase(column, \"this is a test\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_phrase(column, 'this is a test')"));
    assertNotNull(
            parser.parse("SELECT * FROM test WHERE match_phrase(`column`, \"this is a test\")"));
    assertNotNull(
            parser.parse("SELECT * FROM test WHERE match_phrase(`column`, 'this is a test')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE match_phrase(column, 100500)"));
  }

  @Test
  public void can_parse_minute_of_day_function() {
    assertNotNull(parser.parse("SELECT minute_of_day(\"12:23:34\");"));
    assertNotNull(parser.parse("SELECT minute_of_day('12:23:34');"));;
    assertNotNull(parser.parse("SELECT minute_of_day(\"2022-12-14 12:23:34\");"));;
    assertNotNull(parser.parse("SELECT minute_of_day('2022-12-14 12:23:34');"));;
  }

  @Test
  public void can_parse_sec_to_time_function() {
    assertNotNull(parser.parse("SELECT sec_to_time(-6897)"));
    assertNotNull(parser.parse("SELECT sec_to_time(6897)"));
    assertNotNull(parser.parse("SELECT sec_to_time(6897.123)"));
  }

  @Test
  public void can_parse_last_day_function() {
    assertNotNull(parser.parse("SELECT last_day(\"2017-06-20\")"));
    assertNotNull(parser.parse("SELECT last_day('2004-01-01 01:01:01')"));
  }

  @Test
  public void can_parse_timestampadd_function() {
    assertNotNull(parser.parse("SELECT TIMESTAMPADD(MINUTE, 1, '2003-01-02')"));
    assertNotNull(parser.parse("SELECT TIMESTAMPADD(WEEK,1,'2003-01-02')"));
  }

  @Test
  public void can_parse_timestampdiff_function() {
    assertNotNull(parser.parse("SELECT TIMESTAMPDIFF(MINUTE, '2003-01-02', '2003-01-02')"));
    assertNotNull(parser.parse("SELECT TIMESTAMPDIFF(WEEK,'2003-01-02','2003-01-02')"));
  }

  @Test
  public void can_parse_to_seconds_function() {
    assertNotNull(parser.parse("SELECT to_seconds(\"2023-02-20\")"));
    assertNotNull(parser.parse("SELECT to_seconds(950501)"));
  }

  @Test
  public void can_parse_wildcard_query_relevance_function() {
    assertNotNull(
        parser.parse("SELECT * FROM test WHERE wildcard_query(column, \"this is a test*\")"));
    assertNotNull(
        parser.parse("SELECT * FROM test WHERE wildcard_query(column, 'this is a test*')"));
    assertNotNull(
        parser.parse("SELECT * FROM test WHERE wildcard_query(`column`, \"this is a test*\")"));
    assertNotNull(
        parser.parse("SELECT * FROM test WHERE wildcard_query(`column`, 'this is a test*')"));
    assertNotNull(
        parser.parse("SELECT * FROM test WHERE wildcard_query(`column`, 'this is a test*', "
            + "boost=1.5, case_insensitive=true, rewrite=\"scoring_boolean\")"));
  }

  @Test
  public void can_parse_nested_function() {
    assertNotNull(
        parser.parse("SELECT NESTED(PATH.INNER_FIELD) FROM TEST"));
    assertNotNull(
        parser.parse("SELECT NESTED('PATH.INNER_FIELD') FROM TEST"));
    assertNotNull(
        parser.parse("SELECT SUM(NESTED(PATH.INNER_FIELD)) FROM TEST"));
    assertNotNull(
        parser.parse("SELECT NESTED(PATH.INNER_FIELD, PATH) FROM TEST"));
    assertNotNull(
        parser.parse(
            "SELECT * FROM TEST WHERE NESTED(PATH.INNER_FIELDS) = 'A'"
        )
    );
    assertNotNull(
        parser.parse(
            "SELECT * FROM TEST WHERE NESTED(PATH.INNER_FIELDS, PATH) = 'A'"
        )
    );
    assertNotNull(
        parser.parse(
        "SELECT FIELD FROM TEST ORDER BY nested(PATH.INNER_FIELD, PATH)"
        )
    );
  }

  @Test
  public void can_parse_yearweek_function() {
    assertNotNull(parser.parse("SELECT yearweek('1987-01-01')"));
    assertNotNull(parser.parse("SELECT yearweek('1987-01-01', 1)"));
  }

  @Test
  public void describe_request_accepts_only_quoted_string_literals() {
    assertAll(
        () -> assertThrows(SyntaxCheckException.class,
            () -> parser.parse("DESCRIBE TABLES LIKE bank")),
        () -> assertThrows(SyntaxCheckException.class,
            () -> parser.parse("DESCRIBE TABLES LIKE %bank%")),
        () -> assertThrows(SyntaxCheckException.class,
            () -> parser.parse("DESCRIBE TABLES LIKE `bank`")),
        () -> assertThrows(SyntaxCheckException.class,
            () -> parser.parse("DESCRIBE TABLES LIKE %bank% COLUMNS LIKE %status%")),
        () -> assertThrows(SyntaxCheckException.class,
            () -> parser.parse("DESCRIBE TABLES LIKE 'bank' COLUMNS LIKE status")),
        () -> assertNotNull(parser.parse("DESCRIBE TABLES LIKE 'bank' COLUMNS LIKE \"status\"")),
        () -> assertNotNull(parser.parse("DESCRIBE TABLES LIKE \"bank\" COLUMNS LIKE 'status'"))
    );
  }

  @Test
  public void show_request_accepts_only_quoted_string_literals() {
    assertAll(
        () -> assertThrows(SyntaxCheckException.class,
            () -> parser.parse("SHOW TABLES LIKE bank")),
        () -> assertThrows(SyntaxCheckException.class,
            () -> parser.parse("SHOW TABLES LIKE %bank%")),
        () -> assertThrows(SyntaxCheckException.class,
            () -> parser.parse("SHOW TABLES LIKE `bank`")),
        () -> assertNotNull(parser.parse("SHOW TABLES LIKE 'bank'")),
        () -> assertNotNull(parser.parse("SHOW TABLES LIKE \"bank\""))
    );
  }

  @ParameterizedTest
  @MethodSource({
      "matchPhraseComplexQueries",
      "matchPhraseGeneratedQueries",
      "generateMatchPhraseQueries",
      "matchPhraseQueryComplexQueries"
  })
  public void canParseComplexMatchPhraseArgsTest(String query) {
    assertNotNull(parser.parse(query));
  }

  @ParameterizedTest
  @MethodSource({
      "generateMatchPhrasePrefixQueries"
  })
  public void canParseComplexMatchPhrasePrefixQueries(String query) {
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
      "SELECT * FROM t WHERE match_phrase(c, 3, cutoff_frequency=0.04, analyzer = english, "
              + "prefix_length=34, fuzziness='auto', minimum_should_match='2<-25% 9<-3')",
      "SELECT * FROM t WHERE match_phrase(c, 3, minimum_should_match='2<-25% 9<-3')",
      "SELECT * FROM t WHERE match_phrase(c, 3, operator='AUTO')"
    );
  }

  @Test
  public void canParseMatchQueryAlternateSyntax() {
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = matchquery('query')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = matchquery(\"query\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = match_query('query')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = match_query(\"query\")"));
  }

  @Test
  public void canParseMatchPhraseAlternateSyntax() {
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = match_phrase('query')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = match_phrase(\"query\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = matchphrase('query')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = matchphrase(\"query\")"));
  }

  @Test
  public void canParseMultiMatchAlternateSyntax() {
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = multi_match('query')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = multi_match(\"query\")"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = multimatch('query')"));
    assertNotNull(parser.parse("SELECT * FROM test WHERE Field = multimatch(\"query\")"));
  }

  private static Stream<String> matchPhraseQueryComplexQueries() {
    return Stream.of(
        "SELECT * FROM t WHERE matchphrasequery(c, 3)",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, fuzziness=AUTO)",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, zero_terms_query=\"all\")",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, lenient=true)",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, lenient='true')",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, operator=xor)",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, cutoff_frequency=0.04)",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, cutoff_frequency=0.04, analyzer = english, "
            + "prefix_length=34, fuzziness='auto', minimum_should_match='2<-25% 9<-3')",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, minimum_should_match='2<-25% 9<-3')",
        "SELECT * FROM t WHERE matchphrasequery(c, 3, operator='AUTO')"
    );
  }

  private static Stream<String> matchPhraseGeneratedQueries() {
    var matchArgs = new HashMap<String, Object[]>();
    matchArgs.put("fuzziness", new String[]{ "AUTO", "AUTO:1,5", "1" });
    matchArgs.put("fuzzy_transpositions", new Boolean[]{ true, false });
    matchArgs.put("operator", new String[]{ "and", "or" });
    matchArgs.put("minimum_should_match",
            new String[]{ "3", "-2", "75%", "-25%", "3<90%", "2<-25% 9<-3" });
    matchArgs.put("analyzer", new String[]{ "standard", "stop", "english" });
    matchArgs.put("zero_terms_query", new String[]{ "none", "all" });
    matchArgs.put("lenient", new Boolean[]{ true, false });
    // deprecated
    matchArgs.put("cutoff_frequency", new Double[]{ .0, 0.001, 1., 42. });
    matchArgs.put("prefix_length", new Integer[]{ 0, 2, 5 });
    matchArgs.put("max_expansions", new Integer[]{ 0, 5, 20 });
    matchArgs.put("boost", new Double[]{ .5, 1., 2.3 });

    return generateQueries("match", matchArgs);
  }

  private static Stream<String> generateMatchPhraseQueries() {
    var matchPhraseArgs = new HashMap<String, Object[]>();
    matchPhraseArgs.put("analyzer", new String[]{ "standard", "stop", "english" });
    matchPhraseArgs.put("max_expansions", new Integer[]{ 0, 5, 20 });
    matchPhraseArgs.put("slop", new Integer[]{ 0, 1, 2 });

    return generateQueries("match_phrase", matchPhraseArgs);
  }

  private static Stream<String> generateMatchPhrasePrefixQueries() {
    return generateQueries("match_phrase_prefix", ImmutableMap.<String, Object[]>builder()
        .put("analyzer", new String[] {"standard", "stop", "english"})
        .put("slop", new Integer[] {0, 1, 2})
        .put("max_expansions", new Integer[] {0, 3, 10})
        .put("zero_terms_query", new String[] {"NONE", "ALL", "NULL"})
        .put("boost", new Float[] {-0.5f, 1.0f, 1.2f})
        .build());
  }

  private static Stream<String> generateQueries(String function,
                                                Map<String, Object[]> functionArgs) {
    var rand = new Random(0);

    class QueryGenerator implements Iterator<String> {

      private int currentQuery = 0;

      private String randomIdentifier() {
        return RandomStringUtils.random(10, 0, 0,true, false, null, rand);
      }

      @Override
      public boolean hasNext() {
        int numQueries = 100;
        return currentQuery < numQueries;
      }

      @Override
      public String next() {
        currentQuery += 1;

        StringBuilder query = new StringBuilder();
        query.append(String.format("SELECT * FROM test WHERE %s(%s, %s", function,
            randomIdentifier(),
            randomIdentifier()));
        var args = new ArrayList<String>();
        for (var pair : functionArgs.entrySet()) {
          if (rand.nextBoolean()) {
            var arg = new StringBuilder();
            arg.append(rand.nextBoolean() ? "," : ", ");
            arg.append(rand.nextBoolean() ? pair.getKey().toLowerCase()
                    : pair.getKey().toUpperCase());
            arg.append(rand.nextBoolean() ? "=" : " = ");
            if (pair.getValue() instanceof String[] || rand.nextBoolean()) {
              var quoteSymbol = rand.nextBoolean() ? '\'' : '"';
              arg.append(quoteSymbol);
              arg.append(pair.getValue()[rand.nextInt(pair.getValue().length)]);
              arg.append(quoteSymbol);
            } else {
              arg.append(pair.getValue()[rand.nextInt(pair.getValue().length)]);
            }
            args.add(arg.toString());
          }
        }
        Collections.shuffle(args, rand);
        for (var arg : args) {
          query.append(arg);
        }
        query.append(rand.nextBoolean() ? ")" : ");");
        return query.toString();
      }
    }

    var it = new QueryGenerator();
    return Streams.stream(it);
  }
}

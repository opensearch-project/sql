/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.antlr;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

public class PPLSyntaxParserTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testSearchCommandShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=t a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandIgnoreSearchKeywordShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandWithMultipleIndicesShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=t,u a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandCrossClusterShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=c:t a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandCrossClusterHiddenShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=c:.t a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandCrossClusterQualifiedShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=c:t.u a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandCrossClusterHiddenQualifiedShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=c:.t.u a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandMatchAllCrossClusterShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=*:t a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandCrossClusterWithMultipleIndicesShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=c:t,d:u,v a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandCrossClusterIgnoreSearchKeywordShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=c:t a=1 b=2");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchFieldsCommandShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=t a=1 b=2 | fields a,b");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchFieldsCommandCrossClusterShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("search source=c:t a=1 b=2 | fields a,b");
    assertNotEquals(null, tree);
  }

  @Test
  public void testSearchCommandWithoutSourceShouldFail() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("Failed to parse query due to offending symbol");

    new PPLSyntaxParser().parse("search a=1");
  }

  @Test
  public void testRareCommandShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t a=1 | rare a");
    assertNotEquals(null, tree);
  }

  @Test
  public void testRareCommandWithGroupByShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t a=1 | rare a by b");
    assertNotEquals(null, tree);
  }

  @Test
  public void testTopCommandWithoutNShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t a=1 | top a");
    assertNotEquals(null, tree);
  }

  @Test
  public void testTopCommandWithNShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t a=1 | top 1 a");
    assertNotEquals(null, tree);
  }

  @Test
  public void testTopCommandWithNAndGroupByShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t a=1 | top 1 a by b");
    assertNotEquals(null, tree);
  }

  @Test
  public void testTopCommandWithoutNAndGroupByShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t a=1 | top a by b");
    assertNotEquals(null, tree);
  }

  @Test
  public void testCanParseMultiMatchRelevanceFunction() {
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match(['address'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match(['address', 'notes'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match([\"*\"], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match([\"address\"], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match([`address`], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match([address], 'query')"));

    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match(['address' ^ 1.0, 'notes' ^ 2.2], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match(['address', 'notes' ^ 1.5], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match(['address', 'notes' 3], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match(['address' ^ .3, 'notes' 3], 'query')"));

    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE multi_match([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query',"
            + "analyzer=keyword, quote_field_suffix=\".exact\", fuzzy_prefix_length = 4)"));
  }

  @Test
  public void testCanParseSimpleQueryStringRelevanceFunction() {
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string(['address'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string(['address', 'notes'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string([\"*\"], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string([\"address\"], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string([`address`], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string([address], 'query')"));

    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string(['address' ^ 1.0, 'notes' ^ 2.2], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string(['address', 'notes' ^ 1.5], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string(['address', 'notes' 3], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string(['address' ^ .3, 'notes' 3], 'query')"));

    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE simple_query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query',"
            + "analyzer=keyword, quote_field_suffix=\".exact\", fuzzy_prefix_length = 4)"));
  }

  @Test
  public void testCanParseQueryStringRelevanceFunction() {
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string(['address'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string(['address', 'notes'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string([\"*\"], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string([\"address\"], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string([`address`], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string([address], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string(['address' ^ 1.0, 'notes' ^ 2.2], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string(['address', 'notes' ^ 1.5], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string(['address', 'notes' 3], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string(['address' ^ .3, 'notes' 3], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query')"));
    assertNotEquals(null, new PPLSyntaxParser().parse(
        "SOURCE=test | WHERE query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query',"
            + "analyzer=keyword, quote_field_suffix=\".exact\", fuzzy_prefix_length = 4)"));
  }

  @Test
  public void testDescribeCommandShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("describe t");
    assertNotEquals(null, tree);
  }

  @Test
  public void testDescribeCommandWithMultipleIndicesShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("describe t,u");
    assertNotEquals(null, tree);
  }

  @Test
  public void testDescribeCommandCrossClusterShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("describe c:t");
    assertNotEquals(null, tree);
  }

  @Test
  public void testDescribeCommandMatchAllCrossClusterShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("describe *:t");
    assertNotEquals(null, tree);
  }

  @Test
  public void testDescribeFieldsCommandShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("describe t | fields a,b");
    assertNotEquals(null, tree);
  }

  @Test
  public void testDescribeCommandWithSourceShouldFail() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("Failed to parse query due to offending symbol");

    new PPLSyntaxParser().parse("describe source=t");
  }

  @Test
  public void testCanParseExtractFunction() {
    String[] parts = List.of("MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY",
            "WEEK", "MONTH", "QUARTER", "YEAR", "SECOND_MICROSECOND",
            "MINUTE_MICROSECOND", "MINUTE_SECOND", "HOUR_MICROSECOND",
            "HOUR_SECOND", "HOUR_MINUTE", "DAY_MICROSECOND",
            "DAY_SECOND", "DAY_MINUTE", "DAY_HOUR", "YEAR_MONTH").toArray(new String[0]);

    for (String part : parts) {
      assertNotNull(new PPLSyntaxParser().parse(
              String.format("SOURCE=test | eval k = extract(%s FROM \"2023-02-06\")", part)));
    }
  }

  @Test
  public void testCanParseGetFormatFunction() {
    String[] types = {"DATE", "DATETIME", "TIME", "TIMESTAMP"};
    String[] formats = {"'USA'", "'JIS'", "'ISO'", "'EUR'", "'INTERNAL'"};

    for (String type : types) {
      for (String format : formats) {
        assertNotNull(new PPLSyntaxParser().parse(
                String.format("SOURCE=test | eval k = get_format(%s, %s)", type, format)));
      }
    }
  }

  @Test
  public void testCannotParseGetFormatFunctionWithBadArg() {
    assertThrows(
            SyntaxCheckException.class,
            () -> new PPLSyntaxParser().parse(
                    "SOURCE=test | eval k = GET_FORMAT(NONSENSE_ARG,'INTERNAL')"));
  }

  @Test
  public void testCanParseTimestampaddFunction() {
    assertNotNull(new PPLSyntaxParser().parse(
            "SOURCE=test | eval k = TIMESTAMPADD(MINUTE, 1, '2003-01-02')"));
    assertNotNull(new PPLSyntaxParser().parse(
            "SOURCE=test | eval k = TIMESTAMPADD(WEEK,1,'2003-01-02')"));
  }

  @Test
  public void testCanParseTimestampdiffFunction() {
    assertNotNull(new PPLSyntaxParser().parse(
            "SOURCE=test | eval k = TIMESTAMPDIFF(MINUTE, '2003-01-02', '2003-01-02')"));
    assertNotNull(new PPLSyntaxParser().parse(
            "SOURCE=test | eval k = TIMESTAMPDIFF(WEEK,'2003-01-02','2003-01-02')"));
  }
}

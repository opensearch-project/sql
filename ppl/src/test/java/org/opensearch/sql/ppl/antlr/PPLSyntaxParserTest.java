/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.antlr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.text.StringContainsInOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

public class PPLSyntaxParserTest {

  @Rule public final ExpectedException exceptionRule = ExpectedException.none();

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
  public void testPerSecondFunctionInTimechartShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t | timechart per_second(a)");
    assertNotEquals(null, tree);
  }

  @Test
  public void testPerMinuteFunctionInTimechartShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t | timechart per_minute(a)");
    assertNotEquals(null, tree);
  }

  @Test
  public void testPerHourFunctionInTimechartShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t | timechart per_hour(a)");
    assertNotEquals(null, tree);
  }

  @Test
  public void testPerDayFunctionInTimechartShouldPass() {
    ParseTree tree = new PPLSyntaxParser().parse("source=t | timechart per_day(a)");
    assertNotEquals(null, tree);
  }

  @Test
  public void testDynamicSourceClauseParseTreeStructure() {
    String query = "source=[myindex, logs, fieldIndex=\"test\", count=100]";
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchPPLParser parser = new OpenSearchPPLParser(new CommonTokenStream(lexer));

    OpenSearchPPLParser.RootContext root = parser.root();
    assertNotNull(root);

    // Navigate to dynamic source clause
    OpenSearchPPLParser.SearchFromContext searchFrom =
        (OpenSearchPPLParser.SearchFromContext)
            root.pplStatement().queryStatement().pplCommands().searchCommand();
    OpenSearchPPLParser.DynamicSourceClauseContext dynamicSource =
        searchFrom.fromClause().dynamicSourceClause();

    // Verify source references size and text
    assertEquals(
        "Should have 2 source references",
        2,
        dynamicSource.sourceReferences().sourceReference().size());
    assertEquals(
        "Source references text should match",
        "myindex,logs",
        dynamicSource.sourceReferences().getText());

    // Verify filter args size and text
    assertEquals(
        "Should have 2 filter args", 2, dynamicSource.sourceFilterArgs().sourceFilterArg().size());
    assertEquals(
        "Filter args text should match",
        "fieldIndex=\"test\",count=100",
        dynamicSource.sourceFilterArgs().getText());
  }

  @Test
  public void testDynamicSourceWithComplexFilters() {
    String query =
        "source=[vpc.flow_logs, api.gateway, region=\"us-east-1\", env=\"prod\", count=5]";
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchPPLParser parser = new OpenSearchPPLParser(new CommonTokenStream(lexer));

    OpenSearchPPLParser.RootContext root = parser.root();
    OpenSearchPPLParser.SearchFromContext searchFrom =
        (OpenSearchPPLParser.SearchFromContext)
            root.pplStatement().queryStatement().pplCommands().searchCommand();
    OpenSearchPPLParser.DynamicSourceClauseContext dynamicSource =
        searchFrom.fromClause().dynamicSourceClause();

    // Verify source references
    assertEquals(
        "Should have 2 source references",
        2,
        dynamicSource.sourceReferences().sourceReference().size());
    assertEquals(
        "Source references text",
        "vpc.flow_logs,api.gateway",
        dynamicSource.sourceReferences().getText());

    // Verify filter args
    assertEquals(
        "Should have 3 filter args", 3, dynamicSource.sourceFilterArgs().sourceFilterArg().size());
    assertEquals(
        "Filter args text",
        "region=\"us-east-1\",env=\"prod\",count=5",
        dynamicSource.sourceFilterArgs().getText());
  }

  @Test
  public void testDynamicSourceWithSingleSource() {
    String query = "source=[ds:myindex, fieldIndex=\"test\"]";
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchPPLParser parser = new OpenSearchPPLParser(new CommonTokenStream(lexer));

    OpenSearchPPLParser.RootContext root = parser.root();
    OpenSearchPPLParser.SearchFromContext searchFrom =
        (OpenSearchPPLParser.SearchFromContext)
            root.pplStatement().queryStatement().pplCommands().searchCommand();
    OpenSearchPPLParser.DynamicSourceClauseContext dynamicSource =
        searchFrom.fromClause().dynamicSourceClause();

    assertEquals(
        "Should have 1 source reference",
        1,
        dynamicSource.sourceReferences().sourceReference().size());
    assertEquals("Source reference text", "ds:myindex", dynamicSource.sourceReferences().getText());

    assertEquals(
        "Should have 1 filter arg", 1, dynamicSource.sourceFilterArgs().sourceFilterArg().size());
    assertEquals(
        "Filter arg text", "fieldIndex=\"test\"", dynamicSource.sourceFilterArgs().getText());
  }

  @Test
  public void testDynamicSourceRequiresAtLeastOneSource() {
    // The grammar requires at least one source reference before optional filter args
    // This test documents that behavior
    exceptionRule.expect(RuntimeException.class);
    new PPLSyntaxParser().parse("source=[fieldIndex=\"httpStatus\", region=\"us-west-2\"]");
  }

  @Test
  public void testDynamicSourceWithDottedNames() {
    String query = "source=[vpc.flow_logs, api.gateway.logs, env=\"prod\"]";
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchPPLParser parser = new OpenSearchPPLParser(new CommonTokenStream(lexer));

    OpenSearchPPLParser.RootContext root = parser.root();
    OpenSearchPPLParser.SearchFromContext searchFrom =
        (OpenSearchPPLParser.SearchFromContext)
            root.pplStatement().queryStatement().pplCommands().searchCommand();
    OpenSearchPPLParser.DynamicSourceClauseContext dynamicSource =
        searchFrom.fromClause().dynamicSourceClause();

    assertEquals(
        "Should have 2 source references",
        2,
        dynamicSource.sourceReferences().sourceReference().size());
    assertEquals(
        "Source references text",
        "vpc.flow_logs,api.gateway.logs",
        dynamicSource.sourceReferences().getText());

    assertEquals(
        "Should have 1 filter arg", 1, dynamicSource.sourceFilterArgs().sourceFilterArg().size());
    assertEquals("Filter arg text", "env=\"prod\"", dynamicSource.sourceFilterArgs().getText());
  }

  @Test
  public void testDynamicSourceWithSimpleFilter() {
    String query = "source=[logs, count=100]";
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchPPLParser parser = new OpenSearchPPLParser(new CommonTokenStream(lexer));

    OpenSearchPPLParser.RootContext root = parser.root();
    OpenSearchPPLParser.SearchFromContext searchFrom =
        (OpenSearchPPLParser.SearchFromContext)
            root.pplStatement().queryStatement().pplCommands().searchCommand();
    OpenSearchPPLParser.DynamicSourceClauseContext dynamicSource =
        searchFrom.fromClause().dynamicSourceClause();

    assertEquals(
        "Should have 1 source reference",
        1,
        dynamicSource.sourceReferences().sourceReference().size());
    assertEquals("Source reference text", "logs", dynamicSource.sourceReferences().getText());

    assertEquals(
        "Should have 1 filter arg", 1, dynamicSource.sourceFilterArgs().sourceFilterArg().size());
    assertEquals("Filter arg text", "count=100", dynamicSource.sourceFilterArgs().getText());
  }

  @Test
  public void testDynamicSourceWithInClause() {
    // Note: The valueList rule expects literalValue which includes strings
    String query = "source=[myindex, fieldIndex IN (\"httpStatus\", \"requestId\")]";
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(query));
    OpenSearchPPLParser parser = new OpenSearchPPLParser(new CommonTokenStream(lexer));

    OpenSearchPPLParser.RootContext root = parser.root();
    assertNotNull("Query should parse successfully", root);

    // Verify IN clause is parsed
    OpenSearchPPLParser.SearchFromContext searchFrom =
        (OpenSearchPPLParser.SearchFromContext)
            root.pplStatement().queryStatement().pplCommands().searchCommand();
    OpenSearchPPLParser.DynamicSourceClauseContext dynamicSource =
        searchFrom.fromClause().dynamicSourceClause();

    assertNotNull("Dynamic source should exist", dynamicSource);
    assertNotNull("Filter args should exist", dynamicSource.sourceFilterArgs());

    // The IN clause should be parsed as a sourceFilterArg
    assertTrue(
        "Should have at least one filter arg with IN clause",
        dynamicSource.sourceFilterArgs().sourceFilterArg().size() >= 1);
  }

  @Test
  public void testSearchCommandWithoutSourceShouldFail() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("is not a valid term at this part of the query");

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
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE multi_match(['address'], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE multi_match(['address', 'notes'], 'query')"));
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE multi_match([\"*\"], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser().parse("SOURCE=test | WHERE multi_match([\"address\"], 'query')"));
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE multi_match([`address`], 'query')"));
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE multi_match([address], 'query')"));

    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE multi_match(['address' ^ 1.0, 'notes' ^ 2.2], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE multi_match(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE multi_match(['address', 'notes' ^ 1.5], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE multi_match(['address', 'notes' 3], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE multi_match(['address' ^ .3, 'notes' 3], 'query')"));

    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE multi_match([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE multi_match([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query',"
                    + "analyzer=keyword, quote_field_suffix=\".exact\", fuzzy_prefix_length = 4)"));
  }

  @Test
  public void testCanParseSimpleQueryStringRelevanceFunction() {
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE simple_query_string(['address'], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE simple_query_string(['address', 'notes'], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser().parse("SOURCE=test | WHERE simple_query_string([\"*\"], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE simple_query_string([\"address\"], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE simple_query_string([`address`], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser().parse("SOURCE=test | WHERE simple_query_string([address], 'query')"));

    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE simple_query_string(['address' ^ 1.0, 'notes' ^ 2.2],"
                    + " 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE simple_query_string(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE simple_query_string(['address', 'notes' ^ 1.5], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE simple_query_string(['address', 'notes' 3], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE simple_query_string(['address' ^ .3, 'notes' 3], 'query')"));

    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE simple_query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2],"
                    + " 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE simple_query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2],"
                    + " 'query',analyzer=keyword, quote_field_suffix=\".exact\","
                    + " fuzzy_prefix_length = 4)"));
  }

  @Test
  public void testCanParseQueryStringRelevanceFunction() {
    assertNotEquals(
        null,
        new PPLSyntaxParser().parse("SOURCE=test | WHERE query_string(['address'], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE query_string(['address', 'notes'], 'query')"));
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE query_string([\"*\"], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser().parse("SOURCE=test | WHERE query_string([\"address\"], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser().parse("SOURCE=test | WHERE query_string([`address`], 'query')"));
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE query_string([address], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE query_string(['address' ^ 1.0, 'notes' ^ 2.2], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE query_string(['address' ^ 1.1, 'notes'], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE query_string(['address', 'notes' ^ 1.5], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE query_string(['address', 'notes' 3], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE query_string(['address' ^ .3, 'notes' 3], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE query_string([\"Tags\" ^ 1.5, Title, `Body` 4.2], 'query',"
                    + "analyzer=keyword, quote_field_suffix=\".exact\", fuzzy_prefix_length = 4)"));
  }

  @Test
  public void testMultiFieldRelevanceFunctionsWithoutFields() {
    // Test multi_match without fields parameter
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE multi_match('query text')"));

    // Test multi_match without fields but with optional parameters
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE multi_match('query text', analyzer='keyword')"));

    // Test simple_query_string without fields parameter
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE simple_query_string('query text')"));

    // Test simple_query_string without fields but with optional parameters
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE simple_query_string('query text', flags='ALL')"));

    // Test query_string without fields parameter
    assertNotEquals(
        null, new PPLSyntaxParser().parse("SOURCE=test | WHERE query_string('query text')"));

    // Test query_string without fields but with optional parameters
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE query_string('query text', default_operator='AND')"));
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
  public void testInvalidOperatorCombinationShouldFail() {
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        StringContainsInOrder.stringContainsInOrder(
            "[<EOF>] is not a valid term at this part of the query: '...= t | where x > y OR' <--"
                + " HERE.",
            "Expecting one of ",
            " possible tokens. Some examples: ",
            "..."));

    new PPLSyntaxParser().parse("source = t | where x > y OR");
  }

  @Test
  public void testCanParseExtractFunction() {
    String[] parts =
        List.of(
                "MICROSECOND",
                "SECOND",
                "MINUTE",
                "HOUR",
                "DAY",
                "WEEK",
                "MONTH",
                "QUARTER",
                "YEAR",
                "SECOND_MICROSECOND",
                "MINUTE_MICROSECOND",
                "MINUTE_SECOND",
                "HOUR_MICROSECOND",
                "HOUR_SECOND",
                "HOUR_MINUTE",
                "DAY_MICROSECOND",
                "DAY_SECOND",
                "DAY_MINUTE",
                "DAY_HOUR",
                "YEAR_MONTH")
            .toArray(new String[0]);

    for (String part : parts) {
      assertNotNull(
          new PPLSyntaxParser()
              .parse(
                  String.format("SOURCE=test | eval k = extract(%s FROM \"2023-02-06\")", part)));
    }
  }

  @Test
  public void testCanParseGetFormatFunction() {
    String[] types = {"DATE", "DATETIME", "TIME", "TIMESTAMP"};
    String[] formats = {"'USA'", "'JIS'", "'ISO'", "'EUR'", "'INTERNAL'"};

    for (String type : types) {
      for (String format : formats) {
        assertNotNull(
            new PPLSyntaxParser()
                .parse(String.format("SOURCE=test | eval k = get_format(%s, %s)", type, format)));
      }
    }
  }

  @Test
  public void testCannotParseGetFormatFunctionWithBadArg() {
    assertThrows(
        SyntaxCheckException.class,
        () ->
            new PPLSyntaxParser()
                .parse("SOURCE=test | eval k = GET_FORMAT(NONSENSE_ARG,'INTERNAL')"));
  }

  @Test
  public void testCanParseTimestampaddFunction() {
    assertNotNull(
        new PPLSyntaxParser()
            .parse("SOURCE=test | eval k = TIMESTAMPADD(MINUTE, 1, '2003-01-02')"));
    assertNotNull(
        new PPLSyntaxParser().parse("SOURCE=test | eval k = TIMESTAMPADD(WEEK,1,'2003-01-02')"));
  }

  @Test
  public void testCanParseTimestampdiffFunction() {
    assertNotNull(
        new PPLSyntaxParser()
            .parse("SOURCE=test | eval k = TIMESTAMPDIFF(MINUTE, '2003-01-02', '2003-01-02')"));
    assertNotNull(
        new PPLSyntaxParser()
            .parse("SOURCE=test | eval k = TIMESTAMPDIFF(WEEK,'2003-01-02','2003-01-02')"));
  }

  @Test
  public void testCanParseFillNullSameValue() {
    assertNotNull(new PPLSyntaxParser().parse("SOURCE=test | fillnull with 0 in a"));
    assertNotNull(new PPLSyntaxParser().parse("SOURCE=test | fillnull with 0 in a, b"));
  }

  @Test
  public void testCanParseFillNullVariousValues() {
    assertNotNull(new PPLSyntaxParser().parse("SOURCE=test | fillnull using a = 0"));
    assertNotNull(new PPLSyntaxParser().parse("SOURCE=test | fillnull using a = 0, b = 1"));
  }

  @Test
  public void testLineCommentShouldPass() {
    assertNotNull(new PPLSyntaxParser().parse("search source=t a=1 b=2 //this is a comment"));
    assertNotNull(new PPLSyntaxParser().parse("search source=t a=1 b=2 // this is a comment "));
    assertNotNull(
        new PPLSyntaxParser()
            .parse(
                """
                    // test is a new line comment \
                    search source=t a=1 b=2 // test is a line comment at the end of ppl command \
                    | fields a,b // this is line comment inner ppl command\
                    ////this is a new line comment
                    """));
  }

  @Test
  public void testBlockCommentShouldPass() {
    assertNotNull(new PPLSyntaxParser().parse("search source=t a=1 b=2 /*block comment*/"));
    assertNotNull(new PPLSyntaxParser().parse("search source=t a=1 b=2 /* block comment */"));
    assertNotNull(
        new PPLSyntaxParser()
            .parse(
                """
                    /*
                    This is a\
                        multiple\
                    line\
                    block\
                        comment */\
                    search /* block comment */ source=t /* block comment */ a=1 b=2
                    |/*
                        This is a\
                            multiple\
                        line\
                        block\
                            comment */ fields a,b /* block comment */ \
                    """));
  }

  @Test
  public void testWhereCommand() {
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x = 1"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x = y"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x OR y"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE true"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE (1 >= 0)"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE (x >= 0)"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE (x < 1) = (y > 1)"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x = (1 + 2) * 3"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x = 1 + 2 * 3"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE (day_of_week_i < 2) OR (day_of_week_i > 5)"));
  }

  @Test
  public void testWhereCommandWithDoubleEqual() {
    // Test that == operator is supported in WHERE clause
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x == 1"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x == y"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE name == 'John'"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE (x == 1) AND (y == 2)"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x == 1 OR y == 2"));
    // Test mixing = and == operators
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x == 1 AND y = 2"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE x = 1 OR y == 2"));
    assertNotEquals(null, new PPLSyntaxParser().parse("SOURCE=test | WHERE (x < 1) == (y > 1)"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse("SOURCE=test | WHERE match('message', 'test query', analyzer='keyword')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE multi_match(['field1', 'field2' ^ 3.2], 'test query',"
                    + " analyzer='keyword')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE simple_query_string(['field1', 'field2' ^ 3.2], 'test query',"
                    + " analyzer='keyword')"));
    assertNotEquals(
        null,
        new PPLSyntaxParser()
            .parse(
                "SOURCE=test | WHERE query_string(['field1', 'field2' ^ 3.2], 'test query',"
                    + " analyzer='keyword')"));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.and;
import static org.opensearch.sql.ast.dsl.AstDSL.between;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.caseWhen;
import static org.opensearch.sql.ast.dsl.AstDSL.dateLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.highlight;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.intervalLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.longLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.not;
import static org.opensearch.sql.ast.dsl.AstDSL.nullLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.or;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.timeLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.timestampLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;
import static org.opensearch.sql.ast.dsl.AstDSL.when;
import static org.opensearch.sql.ast.dsl.AstDSL.window;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.stream.Stream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.common.antlr.CaseInsensitiveCharStream;
import org.opensearch.sql.common.antlr.SyntaxAnalysisErrorListener;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLLexer;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;

class AstExpressionBuilderTest {

  private final AstExpressionBuilder astExprBuilder = new AstExpressionBuilder();

  @Test
  public void canBuildStringLiteral() {
    assertEquals(stringLiteral("hello"), buildExprAst("'hello'"));
    assertEquals(stringLiteral("hello"), buildExprAst("\"hello\""));
  }

  @Test
  public void canBuildIntegerLiteral() {
    assertEquals(intLiteral(123), buildExprAst("123"));
    assertEquals(intLiteral(Integer.MAX_VALUE), buildExprAst(String.valueOf(Integer.MAX_VALUE)));
    assertEquals(intLiteral(Integer.MIN_VALUE), buildExprAst(String.valueOf(Integer.MIN_VALUE)));
  }

  @Test
  public void canBuildLongLiteral() {
    assertEquals(longLiteral(1234567890123L), buildExprAst("1234567890123"));
    assertEquals(
        longLiteral(Integer.MAX_VALUE + 1L), buildExprAst(String.valueOf(Integer.MAX_VALUE + 1L)));
    assertEquals(
        longLiteral(Integer.MIN_VALUE - 1L), buildExprAst(String.valueOf(Integer.MIN_VALUE - 1L)));
  }

  @Test
  public void canBuildNegativeRealLiteral() {
    assertEquals(doubleLiteral(-4.567), buildExprAst("-4.567"));
  }

  @Test
  public void canBuildBooleanLiteral() {
    assertEquals(booleanLiteral(true), buildExprAst("true"));
  }

  @Test
  public void canBuildDateLiteral() {
    assertEquals(dateLiteral("2020-07-07"), buildExprAst("DATE '2020-07-07'"));
  }

  @Test
  public void canBuildTimeLiteral() {
    assertEquals(timeLiteral("11:30:45"), buildExprAst("TIME '11:30:45'"));
  }

  @Test
  public void canBuildTimestampLiteral() {
    assertEquals(
        timestampLiteral("2020-07-07 11:30:45"), buildExprAst("TIMESTAMP '2020-07-07 11:30:45'"));
  }

  @Test
  public void canBuildIntervalLiteral() {
    assertEquals(intervalLiteral(1, DataType.INTEGER, "day"), buildExprAst("interval 1 day"));
  }

  @Test
  public void canBuildArithmeticExpression() {
    assertEquals(function("+", intLiteral(1), intLiteral(2)), buildExprAst("1 + 2"));
  }

  @Test
  public void canBuildArithmeticExpressionPrecedence() {
    assertEquals(
        function("+", intLiteral(1), function("*", intLiteral(2), intLiteral(3))),
        buildExprAst("1 + 2 * 3"));
  }

  @Test
  public void canBuildFunctionWithoutArguments() {
    assertEquals(function("PI"), buildExprAst("PI()"));
  }

  @Test
  public void canBuildExpressionWithParentheses() {
    assertEquals(
        function(
            "*",
            function("+", doubleLiteral(-1.0), doubleLiteral(2.3)),
            function("-", intLiteral(3), intLiteral(1))),
        buildExprAst("(-1.0 + 2.3) * (3 - 1)"));
  }

  @Test
  public void canBuildFunctionCall() {
    assertEquals(function("abs", intLiteral(-1)), buildExprAst("abs(-1)"));
  }

  @Test
  public void canBuildExtractFunctionCall() {
    assertEquals(
        function("extract", stringLiteral("DAY"), dateLiteral("2023-02-09")).toString(),
        buildExprAst("extract(DAY FROM \"2023-02-09\")").toString());
  }

  @Test
  public void canBuildGetFormatFunctionCall() {
    assertEquals(
        function("get_format", stringLiteral("DATE"), stringLiteral("USA")),
        buildExprAst("get_format(DATE,\"USA\")"));
  }

  @Test
  public void canBuildNestedFunctionCall() {
    assertEquals(
        function("abs", function("*", function("abs", intLiteral(-5)), intLiteral(-1))),
        buildExprAst("abs(abs(-5) * -1)"));
  }

  @Test
  public void canBuildDateAndTimeFunctionCall() {
    assertEquals(
        function("dayofmonth", dateLiteral("2020-07-07")),
        buildExprAst("dayofmonth(DATE '2020-07-07')"));
  }

  @Test
  public void canBuildTimestampAddFunctionCall() {
    assertEquals(
        function("timestampadd", stringLiteral("WEEK"), intLiteral(1), dateLiteral("2023-03-14")),
        buildExprAst("timestampadd(WEEK, 1, DATE '2023-03-14')"));
  }

  @Test
  public void canBuildTimstampDiffFunctionCall() {
    assertEquals(
        function(
            "timestampdiff",
            stringLiteral("WEEK"),
            timestampLiteral("2023-03-15 00:00:01"),
            dateLiteral("2023-03-14")),
        buildExprAst("timestampdiff(WEEK, TIMESTAMP '2023-03-15 00:00:01', DATE '2023-03-14')"));
  }

  @Test
  public void canBuildComparisonExpression() {
    assertEquals(function("!=", intLiteral(1), intLiteral(2)), buildExprAst("1 != 2"));

    assertEquals(function("!=", intLiteral(1), intLiteral(2)), buildExprAst("1 <> 2"));
  }

  @Test
  public void canBuildNullTestExpression() {
    assertEquals(function("is null", intLiteral(1)), buildExprAst("1 is NULL"));

    assertEquals(function("is not null", intLiteral(1)), buildExprAst("1 IS NOT null"));
  }

  @Test
  public void canBuildNullTestExpressionWithNULLLiteral() {
    assertEquals(function("is null", nullLiteral()), buildExprAst("NULL is NULL"));

    assertEquals(function("is not null", nullLiteral()), buildExprAst("NULL IS NOT null"));
  }

  @Test
  public void canBuildLikeExpression() {
    assertEquals(
        function("like", stringLiteral("str"), stringLiteral("st%")),
        buildExprAst("'str' like 'st%'"));

    assertEquals(
        function("not like", stringLiteral("str"), stringLiteral("st%")),
        buildExprAst("'str' not like 'st%'"));
  }

  @Test
  public void canBuildRegexpExpression() {
    assertEquals(
        function("regexp", stringLiteral("str"), stringLiteral(".*")),
        buildExprAst("'str' regexp '.*'"));
  }

  @Test
  public void canBuildBetweenExpression() {
    assertEquals(
        between(qualifiedName("age"), intLiteral(10), intLiteral(30)),
        buildExprAst("age BETWEEN 10 AND 30"));
  }

  @Test
  public void canBuildNotBetweenExpression() {
    assertEquals(
        not(between(qualifiedName("age"), intLiteral(10), intLiteral(30))),
        buildExprAst("age NOT BETWEEN 10 AND 30"));
  }

  @Test
  public void canBuildLogicalExpression() {
    assertEquals(and(booleanLiteral(true), booleanLiteral(false)), buildExprAst("true AND false"));

    assertEquals(or(booleanLiteral(true), booleanLiteral(false)), buildExprAst("true OR false"));

    assertEquals(not(booleanLiteral(false)), buildExprAst("NOT false"));
  }

  @Test
  public void canBuildWindowFunction() {
    assertEquals(
        window(
            function("RANK"),
            ImmutableList.of(qualifiedName("state")),
            ImmutableList.of(ImmutablePair.of(new SortOption(null, null), qualifiedName("age")))),
        buildExprAst("RANK() OVER (PARTITION BY state ORDER BY age)"));
  }

  @Test
  public void canBuildWindowFunctionWithoutPartitionBy() {
    assertEquals(
        window(
            function("DENSE_RANK"),
            ImmutableList.of(),
            ImmutableList.of(ImmutablePair.of(new SortOption(DESC, null), qualifiedName("age")))),
        buildExprAst("DENSE_RANK() OVER (ORDER BY age DESC)"));
  }

  @Test
  public void canBuildWindowFunctionWithNullOrderSpecified() {
    assertEquals(
        window(
            function("DENSE_RANK"),
            ImmutableList.of(),
            ImmutableList.of(
                ImmutablePair.of(new SortOption(ASC, NULL_LAST), qualifiedName("age")))),
        buildExprAst("DENSE_RANK() OVER (ORDER BY age ASC NULLS LAST)"));
  }

  @Test
  public void canBuildStringLiteralHighlightFunction() {
    HashMap<String, Literal> args = new HashMap<>();
    assertEquals(
        highlight(AstDSL.stringLiteral("fieldA"), args), buildExprAst("highlight(\"fieldA\")"));
  }

  @Test
  public void canBuildQualifiedNameHighlightFunction() {
    HashMap<String, Literal> args = new HashMap<>();
    assertEquals(
        highlight(AstDSL.qualifiedName("fieldA"), args), buildExprAst("highlight(fieldA)"));
  }

  @Test
  public void canBuildStringLiteralPositionFunction() {
    assertEquals(
        function("position", stringLiteral("substr"), stringLiteral("str")),
        buildExprAst("position(\"substr\" IN \"str\")"));
  }

  @Test
  public void canBuildWindowFunctionWithoutOrderBy() {
    assertEquals(
        window(function("RANK"), ImmutableList.of(qualifiedName("state")), ImmutableList.of()),
        buildExprAst("RANK() OVER (PARTITION BY state)"));
  }

  @Test
  public void canBuildAggregateWindowFunction() {
    assertEquals(
        window(
            aggregate("AVG", qualifiedName("age")),
            ImmutableList.of(qualifiedName("state")),
            ImmutableList.of(ImmutablePair.of(new SortOption(null, null), qualifiedName("age")))),
        buildExprAst("AVG(age) OVER (PARTITION BY state ORDER BY age)"));
  }

  @Test
  public void canBuildCaseConditionStatement() {
    assertEquals(
        caseWhen(
            null, // no else statement
            when(function(">", qualifiedName("age"), intLiteral(30)), stringLiteral("age1"))),
        buildExprAst("CASE WHEN age > 30 THEN 'age1' END"));
  }

  @Test
  public void canBuildCaseValueStatement() {
    assertEquals(
        caseWhen(
            qualifiedName("age"),
            stringLiteral("age2"),
            when(intLiteral(30), stringLiteral("age1"))),
        buildExprAst("CASE age WHEN 30 THEN 'age1' ELSE 'age2' END"));
  }

  @Test
  public void canBuildKeywordsAsIdentifiers() {
    assertEquals(qualifiedName("timestamp"), buildExprAst("timestamp"));
  }

  @Test
  public void canBuildKeywordsAsIdentInQualifiedName() {
    assertEquals(qualifiedName("test", "timestamp"), buildExprAst("test.timestamp"));
  }

  @Test
  public void canBuildMetaDataFieldAsQualifiedName() {
    Stream.of("_id", "_index", "_sort", "_score", "_maxscore")
        .forEach(field -> assertEquals(qualifiedName(field), buildExprAst(field)));
  }

  @Test
  public void canBuildNonMetaDataFieldAsQualifiedName() {
    Stream.of("id", "__id", "_routing", "___field")
        .forEach(field -> assertEquals(qualifiedName(field), buildExprAst(field)));
  }

  @Test
  public void canCastFieldAsString() {
    assertEquals(
        AstDSL.cast(qualifiedName("state"), stringLiteral("string")),
        buildExprAst("cast(state as string)"));
  }

  @Test
  public void canCastValueAsString() {
    assertEquals(
        AstDSL.cast(intLiteral(1), stringLiteral("string")), buildExprAst("cast(1 as string)"));
  }

  @Test
  public void filteredAggregation() {
    assertEquals(
        AstDSL.filteredAggregate(
            "avg", qualifiedName("age"), function(">", qualifiedName("age"), intLiteral(20))),
        buildExprAst("avg(age) filter(where age > 20)"));
  }

  @Test
  public void canBuildVarSamp() {
    assertEquals(aggregate("var_samp", qualifiedName("age")), buildExprAst("var_samp(age)"));
  }

  @Test
  public void canBuildVarPop() {
    assertEquals(aggregate("var_pop", qualifiedName("age")), buildExprAst("var_pop(age)"));
  }

  @Test
  public void canBuildVariance() {
    assertEquals(aggregate("variance", qualifiedName("age")), buildExprAst("variance(age)"));
  }

  @Test
  public void distinctCount() {
    assertEquals(
        AstDSL.distinctAggregate("count", qualifiedName("name")),
        buildExprAst("count(distinct name)"));
  }

  @Test
  public void filteredDistinctCount() {
    assertEquals(
        AstDSL.filteredDistinctCount(
            "count", qualifiedName("name"), function(">", qualifiedName("age"), intLiteral(30))),
        buildExprAst("count(distinct name) filter(where age > 30)"));
  }

  @Test
  public void canBuildPercentile() {
    Object expected =
        aggregate(
            "percentile", qualifiedName("age"), unresolvedArg("quantile", doubleLiteral(50D)));
    assertEquals(expected, buildExprAst("percentile(age, 50)"));
    assertEquals(expected, buildExprAst("percentile(age, 50.0)"));
  }

  @Test
  public void canBuildPercentileWithCompression() {
    Object expected =
        aggregate(
            "percentile",
            qualifiedName("age"),
            unresolvedArg("quantile", doubleLiteral(50D)),
            unresolvedArg("compression", doubleLiteral(100D)));
    assertEquals(expected, buildExprAst("percentile(age, 50, 100)"));
    assertEquals(expected, buildExprAst("percentile(age, 50.0, 100.0)"));
  }

  @Test
  public void matchPhraseQueryAllParameters() {
    assertEquals(
        AstDSL.function(
            "matchphrasequery",
            unresolvedArg("field", qualifiedName("test")),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("slop", stringLiteral("3")),
            unresolvedArg("analyzer", stringLiteral("standard")),
            unresolvedArg("zero_terms_query", stringLiteral("NONE"))),
        buildExprAst(
            "matchphrasequery(test, 'search query', slop = 3"
                + ", analyzer = 'standard', zero_terms_query='NONE'"
                + ")"));
  }

  @Test
  public void matchPhrasePrefixAllParameters() {
    assertEquals(
        AstDSL.function(
            "match_phrase_prefix",
            unresolvedArg("field", qualifiedName("test")),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("slop", stringLiteral("3")),
            unresolvedArg("boost", stringLiteral("1.5")),
            unresolvedArg("analyzer", stringLiteral("standard")),
            unresolvedArg("max_expansions", stringLiteral("4")),
            unresolvedArg("zero_terms_query", stringLiteral("NONE"))),
        buildExprAst(
            "match_phrase_prefix(test, 'search query', slop = 3, boost = 1.5"
                + ", analyzer = 'standard', max_expansions = 4, zero_terms_query='NONE'"
                + ")"));
  }

  @Test
  public void relevanceMatch() {
    assertEquals(
        AstDSL.function(
            "match",
            unresolvedArg("field", qualifiedName("message")),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("match('message', 'search query')"));

    assertEquals(
        AstDSL.function(
            "match",
            unresolvedArg("field", qualifiedName("message")),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("operator", stringLiteral("AND"))),
        buildExprAst("match('message', 'search query', analyzer='keyword', operator='AND')"));
  }

  @Test
  public void relevanceMatchQuery() {
    assertEquals(
        AstDSL.function(
            "matchquery",
            unresolvedArg("field", qualifiedName("message")),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("matchquery('message', 'search query')"));

    assertEquals(
        AstDSL.function(
            "matchquery",
            unresolvedArg("field", qualifiedName("message")),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("operator", stringLiteral("AND"))),
        buildExprAst("matchquery('message', 'search query', analyzer='keyword', operator='AND')"));
  }

  @Test
  public void relevanceMatch_Query() {
    assertEquals(
        AstDSL.function(
            "match_query",
            unresolvedArg("field", qualifiedName("message")),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("match_query('message', 'search query')"));

    assertEquals(
        AstDSL.function(
            "match_query",
            unresolvedArg("field", qualifiedName("message")),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("operator", stringLiteral("AND"))),
        buildExprAst("match_query('message', 'search query', analyzer='keyword', operator='AND')"));
  }

  @Test
  public void relevanceMatchQueryAltSyntax() {
    assertEquals(
        AstDSL.function(
                "match_query",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("search query")))
            .toString(),
        buildExprAst("message = match_query('search query')").toString());

    assertEquals(
        AstDSL.function(
                "match_query",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("search query")))
            .toString(),
        buildExprAst("message = match_query(\"search query\")").toString());

    assertEquals(
        AstDSL.function(
                "matchquery",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("search query")))
            .toString(),
        buildExprAst("message = matchquery('search query')").toString());

    assertEquals(
        AstDSL.function(
                "matchquery",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("search query")))
            .toString(),
        buildExprAst("message = matchquery(\"search query\")").toString());
  }

  @Test
  public void relevanceMatchPhraseAltSyntax() {
    assertEquals(
        AstDSL.function(
                "match_phrase",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("search query")))
            .toString(),
        buildExprAst("message = match_phrase('search query')").toString());

    assertEquals(
        AstDSL.function(
                "match_phrase",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("search query")))
            .toString(),
        buildExprAst("message = match_phrase(\"search query\")").toString());

    assertEquals(
        AstDSL.function(
                "matchphrase",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("search query")))
            .toString(),
        buildExprAst("message = matchphrase('search query')").toString());

    assertEquals(
        AstDSL.function(
                "matchphrase",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("search query")))
            .toString(),
        buildExprAst("message = matchphrase(\"search query\")").toString());
  }

  @Test
  public void relevanceMultiMatchAltSyntax() {
    assertEquals(
        AstDSL.function(
            "multi_match",
            unresolvedArg("fields", new RelevanceFieldList(ImmutableMap.of("field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("field1 = multi_match('search query')"));

    assertEquals(
        AstDSL.function(
            "multi_match",
            unresolvedArg("fields", new RelevanceFieldList(ImmutableMap.of("field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("field1 = multi_match(\"search query\")"));

    assertEquals(
        AstDSL.function(
            "multimatch",
            unresolvedArg("fields", new RelevanceFieldList(ImmutableMap.of("field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("field1 = multimatch('search query')"));

    assertEquals(
        AstDSL.function(
            "multimatch",
            unresolvedArg("fields", new RelevanceFieldList(ImmutableMap.of("field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("field1 = multimatch(\"search query\")"));
  }

  @Test
  public void relevanceMulti_match() {
    assertEquals(
        AstDSL.function(
            "multi_match",
            unresolvedArg(
                "fields", new RelevanceFieldList(ImmutableMap.of("field2", 3.2F, "field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("multi_match(['field1', 'field2' ^ 3.2], 'search query')"));

    assertEquals(
        AstDSL.function(
            "multi_match",
            unresolvedArg(
                "fields", new RelevanceFieldList(ImmutableMap.of("field2", 3.2F, "field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("operator", stringLiteral("AND"))),
        buildExprAst(
            "multi_match(['field1', 'field2' ^ 3.2], 'search query',"
                + "analyzer='keyword', 'operator'='AND')"));
  }

  @Test
  public void relevanceMultimatch_alternate_parameter_syntax() {
    assertEquals(
        AstDSL.function(
            "multimatch",
            unresolvedArg(
                "fields", new RelevanceFieldList(ImmutableMap.of("field1", 1F, "field2", 2F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("multimatch(query='search query', fields=['field1^1.0,field2^2.0'])"));

    assertEquals(
        AstDSL.function(
            "multimatch",
            unresolvedArg(
                "fields", new RelevanceFieldList(ImmutableMap.of("field1", 1F, "field2", 2F))),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("operator", stringLiteral("AND"))),
        buildExprAst(
            "multimatch(query='search query', fields=['field1^1.0,field2^2.0'],"
                + "analyzer='keyword', operator='AND')"));
  }

  @Test
  public void relevanceMultimatchquery_alternate_parameter_syntax() {
    assertEquals(
        AstDSL.function(
            "multimatchquery",
            unresolvedArg("fields", new RelevanceFieldList(ImmutableMap.of("field", 1F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("multimatchquery(query='search query', fields='field')"));

    assertEquals(
        AstDSL.function(
            "multimatchquery",
            unresolvedArg("fields", new RelevanceFieldList(ImmutableMap.of("field", 1F))),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("operator", stringLiteral("AND"))),
        buildExprAst(
            "multimatchquery(query='search query', fields='field',"
                + "analyzer='keyword', 'operator'='AND')"));
  }

  @Test
  public void relevanceSimple_query_string() {
    assertEquals(
        AstDSL.function(
            "simple_query_string",
            unresolvedArg(
                "fields", new RelevanceFieldList(ImmutableMap.of("field2", 3.2F, "field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("simple_query_string(['field1', 'field2' ^ 3.2], 'search query')"));

    assertEquals(
        AstDSL.function(
            "simple_query_string",
            unresolvedArg(
                "fields", new RelevanceFieldList(ImmutableMap.of("field2", 3.2F, "field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("operator", stringLiteral("AND"))),
        buildExprAst(
            "simple_query_string(['field1', 'field2' ^ 3.2], 'search query',"
                + "analyzer='keyword', operator='AND')"));
  }

  @Test
  public void relevanceQuery_string() {
    assertEquals(
        AstDSL.function(
            "query_string",
            unresolvedArg(
                "fields", new RelevanceFieldList(ImmutableMap.of("field2", 3.2F, "field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query"))),
        buildExprAst("query_string(['field1', 'field2' ^ 3.2], 'search query')"));

    assertEquals(
        AstDSL.function(
            "query_string",
            unresolvedArg(
                "fields", new RelevanceFieldList(ImmutableMap.of("field2", 3.2F, "field1", 1.F))),
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("time_zone", stringLiteral("Canada/Pacific")),
            unresolvedArg("tie_breaker", stringLiteral("1.3"))),
        buildExprAst(
            "query_string(['field1', 'field2' ^ 3.2], 'search query',"
                + "analyzer='keyword', time_zone='Canada/Pacific', tie_breaker='1.3')"));
  }

  @Test
  public void relevanceWildcard_query() {
    assertEquals(
        AstDSL.function(
            "wildcard_query",
            unresolvedArg("field", qualifiedName("field")),
            unresolvedArg("query", stringLiteral("search query*")),
            unresolvedArg("boost", stringLiteral("1.5")),
            unresolvedArg("case_insensitive", stringLiteral("true")),
            unresolvedArg("rewrite", stringLiteral("scoring_boolean"))),
        buildExprAst(
            "wildcard_query(field, 'search query*', boost=1.5,"
                + "case_insensitive=true, rewrite='scoring_boolean'))"));
  }

  @Test
  public void relevanceScore_query() {
    assertEquals(
        AstDSL.score(
            AstDSL.function(
                "query_string",
                unresolvedArg(
                    "fields",
                    new RelevanceFieldList(ImmutableMap.of("field1", 1.F, "field2", 3.2F))),
                unresolvedArg("query", stringLiteral("search query"))),
            AstDSL.doubleLiteral(1.0)),
        buildExprAst("score(query_string(['field1', 'field2' ^ 3.2], 'search query'))"));
  }

  @Test
  public void relevanceScore_withBoost_query() {
    assertEquals(
        AstDSL.score(
            AstDSL.function(
                "query_string",
                unresolvedArg(
                    "fields",
                    new RelevanceFieldList(ImmutableMap.of("field1", 1.F, "field2", 3.2F))),
                unresolvedArg("query", stringLiteral("search query"))),
            doubleLiteral(1.0)),
        buildExprAst("score(query_string(['field1', 'field2' ^ 3.2], 'search query'), 1.0)"));
  }

  @Test
  public void relevanceQuery() {
    assertEquals(
        AstDSL.function(
            "query", unresolvedArg("query", stringLiteral("field1:query OR field2:query"))),
        buildExprAst("query('field1:query OR field2:query')"));

    assertEquals(
        AstDSL.function(
            "query",
            unresolvedArg("query", stringLiteral("search query")),
            unresolvedArg("analyzer", stringLiteral("keyword")),
            unresolvedArg("time_zone", stringLiteral("Canada/Pacific")),
            unresolvedArg("tie_breaker", stringLiteral("1.3"))),
        buildExprAst(
            "query('search query',"
                + "analyzer='keyword', time_zone='Canada/Pacific', tie_breaker='1.3')"));
  }

  @Test
  public void canBuildInClause() {
    assertEquals(
        AstDSL.in(qualifiedName("age"), AstDSL.intLiteral(20), AstDSL.intLiteral(30)),
        buildExprAst("age in (20, 30)"));

    assertEquals(
        AstDSL.not(AstDSL.in(qualifiedName("age"), AstDSL.intLiteral(20), AstDSL.intLiteral(30))),
        buildExprAst("age not in (20, 30)"));

    assertEquals(
        AstDSL.in(
            qualifiedName("age"),
            AstDSL.function("abs", AstDSL.intLiteral(20)),
            AstDSL.function("abs", AstDSL.intLiteral(30))),
        buildExprAst("age in (abs(20), abs(30))"));
  }

  private Node buildExprAst(String expr) {
    OpenSearchSQLLexer lexer = new OpenSearchSQLLexer(new CaseInsensitiveCharStream(expr));
    OpenSearchSQLParser parser = new OpenSearchSQLParser(new CommonTokenStream(lexer));
    parser.addErrorListener(new SyntaxAnalysisErrorListener());
    return parser.expression().accept(astExprBuilder);
  }
}

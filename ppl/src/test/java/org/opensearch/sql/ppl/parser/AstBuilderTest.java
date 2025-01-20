/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.computation;
import static org.opensearch.sql.ast.dsl.AstDSL.dedupe;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultDedupArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultFieldsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultSortFieldArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultStatsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.head;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.map;
import static org.opensearch.sql.ast.dsl.AstDSL.nullLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.parse;
import static org.opensearch.sql.ast.dsl.AstDSL.projectWithArg;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.rareTopN;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.rename;
import static org.opensearch.sql.ast.dsl.AstDSL.sort;
import static org.opensearch.sql.ast.dsl.AstDSL.span;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.tableFunction;
import static org.opensearch.sql.ast.dsl.AstDSL.trendline;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;
import static org.opensearch.sql.ast.tree.Trendline.TrendlineType.SMA;
import static org.opensearch.sql.utils.SystemIndexUtils.DATASOURCES_TABLE_NAME;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.ML;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

public class AstBuilderTest {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  private final PPLSyntaxParser parser = new PPLSyntaxParser();

  @Test
  public void testSearchCommand() {
    assertEqual(
        "search source=t a=1", filter(relation("t"), compare("=", field("a"), intLiteral(1))));
  }

  @Test
  public void testSearchCrossClusterCommand() {
    assertEqual("search source=c:t", relation(qualifiedName("c:t")));
  }

  @Test
  public void testSearchMatchAllCrossClusterCommand() {
    assertEqual("search source=*:t", relation(qualifiedName("*:t")));
  }

  @Test
  public void testPrometheusSearchCommand() {
    assertEqual(
        "search source = prometheus.http_requests_total",
        relation(qualifiedName("prometheus", "http_requests_total")));
  }

  @Test
  public void testSearchCommandWithDataSourceEscape() {
    assertEqual(
        "search source = `prometheus.http_requests_total`",
        relation("prometheus.http_requests_total"));
  }

  @Test
  public void testSearchCommandWithDotInIndexName() {
    assertEqual(
        "search source = http_requests_total.test",
        relation(qualifiedName("http_requests_total", "test")));
  }

  @Test
  public void testSearchWithPrometheusQueryRangeWithPositionedArguments() {
    assertEqual(
        "search source = prometheus.query_range(\"test{code='200'}\",1234, 12345, 3)",
        tableFunction(
            Arrays.asList("prometheus", "query_range"),
            unresolvedArg(null, stringLiteral("test{code='200'}")),
            unresolvedArg(null, intLiteral(1234)),
            unresolvedArg(null, intLiteral(12345)),
            unresolvedArg(null, intLiteral(3))));
  }

  @Test
  public void testSearchWithPrometheusQueryRangeWithNamedArguments() {
    assertEqual(
        "search source = prometheus.query_range(query = \"test{code='200'}\", "
            + "starttime = 1234, step=3, endtime=12345)",
        tableFunction(
            Arrays.asList("prometheus", "query_range"),
            unresolvedArg("query", stringLiteral("test{code='200'}")),
            unresolvedArg("starttime", intLiteral(1234)),
            unresolvedArg("step", intLiteral(3)),
            unresolvedArg("endtime", intLiteral(12345))));
  }

  @Test
  public void testSearchCommandString() {
    assertEqual(
        "search source=t a=\"a\"",
        filter(relation("t"), compare("=", field("a"), stringLiteral("a"))));
  }

  @Test
  public void testSearchCommandWithoutSearch() {
    assertEqual("source=t a=1", filter(relation("t"), compare("=", field("a"), intLiteral(1))));
  }

  @Test
  public void testSearchCommandWithFilterBeforeSource() {
    assertEqual(
        "search a=1 source=t", filter(relation("t"), compare("=", field("a"), intLiteral(1))));
  }

  @Test
  public void testWhereCommand() {
    assertEqual(
        "search source=t | where a=1",
        filter(relation("t"), compare("=", field("a"), intLiteral(1))));
  }

  @Test
  public void testWhereCommandWithQualifiedName() {
    assertEqual(
        "search source=t | where a.v=1",
        filter(relation("t"), compare("=", field(qualifiedName("a", "v")), intLiteral(1))));
  }

  @Test
  public void testFieldsCommandWithoutArguments() {
    assertEqual(
        "source=t | fields f, g",
        projectWithArg(relation("t"), defaultFieldsArgs(), field("f"), field("g")));
  }

  @Test
  public void testFieldsCommandWithIncludeArguments() {
    assertEqual(
        "source=t | fields + f, g",
        projectWithArg(relation("t"), defaultFieldsArgs(), field("f"), field("g")));
  }

  @Test
  public void testFieldsCommandWithExcludeArguments() {
    assertEqual(
        "source=t | fields - f, g",
        projectWithArg(
            relation("t"),
            exprList(argument("exclude", booleanLiteral(true))),
            field("f"),
            field("g")));
  }

  @Test
  public void testSearchCommandWithQualifiedName() {
    assertEqual(
        "source=t | fields f.v, g.v",
        projectWithArg(
            relation("t"),
            defaultFieldsArgs(),
            field(qualifiedName("f", "v")),
            field(qualifiedName("g", "v"))));
  }

  @Test
  public void testRenameCommand() {
    assertEqual("source=t | rename f as g", rename(relation("t"), map("f", "g")));
  }

  @Test
  public void testRenameCommandWithMultiFields() {
    assertEqual(
        "source=t | rename f as g, h as i, j as k",
        rename(relation("t"), map("f", "g"), map("h", "i"), map("j", "k")));
  }

  @Test
  public void testStatsCommand() {
    assertEqual(
        "source=t | stats count(a)",
        agg(
            relation("t"),
            exprList(alias("count(a)", aggregate("count", field("a")))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void testStatsCommandWithByClause() {
    assertEqual(
        "source=t | stats count(a) by b DEDUP_SPLITVALUES=false",
        agg(
            relation("t"),
            exprList(alias("count(a)", aggregate("count", field("a")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            defaultStatsArgs()));
  }

  @Test
  public void testStatsCommandWithByClauseInBackticks() {
    assertEqual(
        "source=t | stats count(a) by `b` DEDUP_SPLITVALUES=false",
        agg(
            relation("t"),
            exprList(alias("count(a)", aggregate("count", field("a")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            defaultStatsArgs()));
  }

  @Test
  public void testStatsCommandWithAlias() {
    assertEqual(
        "source=t | stats count(a) as alias",
        agg(
            relation("t"),
            exprList(alias("alias", aggregate("count", field("a")))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void testStatsCommandWithNestedFunctions() {
    assertEqual(
        "source=t | stats sum(a+b)",
        agg(
            relation("t"),
            exprList(alias("sum(a+b)", aggregate("sum", function("+", field("a"), field("b"))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
    assertEqual(
        "source=t | stats sum(abs(a)/2)",
        agg(
            relation("t"),
            exprList(
                alias(
                    "sum(abs(a)/2)",
                    aggregate("sum", function("/", function("abs", field("a")), intLiteral(2))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void testStatsCommandWithSpan() {
    assertEqual(
        "source=t | stats avg(price) by span(timestamp, 1h)",
        agg(
            relation("t"),
            exprList(alias("avg(price)", aggregate("avg", field("price")))),
            emptyList(),
            emptyList(),
            alias("span(timestamp,1h)", span(field("timestamp"), intLiteral(1), SpanUnit.H)),
            defaultStatsArgs()));

    assertEqual(
        "source=t | stats count(a) by span(age, 10)",
        agg(
            relation("t"),
            exprList(alias("count(a)", aggregate("count", field("a")))),
            emptyList(),
            emptyList(),
            alias("span(age,10)", span(field("age"), intLiteral(10), SpanUnit.NONE)),
            defaultStatsArgs()));

    assertEqual(
        "source=t | stats avg(price) by span(timestamp, 1h), b",
        agg(
            relation("t"),
            exprList(alias("avg(price)", aggregate("avg", field("price")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            alias("span(timestamp,1h)", span(field("timestamp"), intLiteral(1), SpanUnit.H)),
            defaultStatsArgs()));

    assertEqual(
        "source=t | stats avg(price) by span(timestamp, 1h), f1, f2",
        agg(
            relation("t"),
            exprList(alias("avg(price)", aggregate("avg", field("price")))),
            emptyList(),
            exprList(alias("f1", field("f1")), alias("f2", field("f2"))),
            alias("span(timestamp,1h)", span(field("timestamp"), intLiteral(1), SpanUnit.H)),
            defaultStatsArgs()));

    assertEqual(
        "source=t | stats avg(price) by b, span(timestamp, 1h)",
        agg(
            relation("t"),
            exprList(alias("avg(price)", aggregate("avg", field("price")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            alias("span(timestamp,1h)", span(field("timestamp"), intLiteral(1), SpanUnit.H)),
            defaultStatsArgs()));

    assertEqual(
        "source=t | stats avg(price) by f1, f2, span(timestamp, 1h)",
        agg(
            relation("t"),
            exprList(alias("avg(price)", aggregate("avg", field("price")))),
            emptyList(),
            exprList(alias("f1", field("f1")), alias("f2", field("f2"))),
            alias("span(timestamp,1h)", span(field("timestamp"), intLiteral(1), SpanUnit.H)),
            defaultStatsArgs()));
  }

  @Test(expected = org.opensearch.sql.common.antlr.SyntaxCheckException.class)
  public void throwExceptionWithEmptyGroupByList() {
    plan("source=t | stats avg(price) by)");
  }

  @Test
  public void testStatsSpanWithAlias() {
    assertEqual(
        "source=t | stats avg(price) by span(timestamp, 1h) as time_span",
        agg(
            relation("t"),
            exprList(alias("avg(price)", aggregate("avg", field("price")))),
            emptyList(),
            emptyList(),
            alias(
                "span(timestamp,1h)",
                span(field("timestamp"), intLiteral(1), SpanUnit.H),
                "time_span"),
            defaultStatsArgs()));

    assertEqual(
        "source=t | stats count(a) by span(age, 10) as numeric_span",
        agg(
            relation("t"),
            exprList(alias("count(a)", aggregate("count", field("a")))),
            emptyList(),
            emptyList(),
            alias(
                "span(age,10)", span(field("age"), intLiteral(10), SpanUnit.NONE), "numeric_span"),
            defaultStatsArgs()));
  }

  @Test
  public void testDedupCommand() {
    assertEqual(
        "source=t | dedup f1, f2",
        dedupe(relation("t"), defaultDedupArgs(), field("f1"), field("f2")));
  }

  /** disable sortby from the dedup command syntax. */
  @Ignore(value = "disable sortby from the dedup command syntax")
  public void testDedupCommandWithSortby() {
    assertEqual(
        "source=t | dedup f1, f2 sortby f3",
        agg(
            relation("t"),
            exprList(field("f1"), field("f2")),
            exprList(field("f3", defaultSortFieldArgs())),
            null,
            defaultDedupArgs()));
  }

  @Test
  public void testHeadCommand() {
    assertEqual("source=t | head", head(relation("t"), 10, 0));
  }

  @Test
  public void testHeadCommandWithNumber() {
    assertEqual("source=t | head 3", head(relation("t"), 3, 0));
  }

  @Test
  public void testHeadCommandWithNumberAndOffset() {
    assertEqual("source=t | head 3 from 4", head(relation("t"), 3, 4));
  }

  @Test
  public void testSortCommand() {
    assertEqual(
        "source=t | sort f1, f2",
        sort(
            relation("t"),
            field("f1", defaultSortFieldArgs()),
            field("f2", defaultSortFieldArgs())));
  }

  @Test
  public void testSortCommandWithOptions() {
    assertEqual(
        "source=t | sort - f1, + f2",
        sort(
            relation("t"),
            field(
                "f1",
                exprList(argument("asc", booleanLiteral(false)), argument("type", nullLiteral()))),
            field("f2", defaultSortFieldArgs())));
  }

  @Test
  public void testEvalCommand() {
    assertEqual(
        "source=t | eval r=abs(f)",
        eval(relation("t"), let(field("r"), function("abs", field("f")))));
  }

  @Test
  public void testFlattenCommand() {

    // TODO #3030: Test
  }

  @Test
  public void testIndexName() {
    assertEqual(
        "source=`log.2020.04.20.` a=1",
        filter(relation("log.2020.04.20."), compare("=", field("a"), intLiteral(1))));
    assertEqual("describe `log.2020.04.20.`", relation(mappingTable("log.2020.04.20.")));
  }

  @Test
  public void testIdentifierAsIndexNameStartWithDot() {
    assertEqual("source=.opensearch_dashboards", relation(".opensearch_dashboards"));
    assertEqual(
        "describe .opensearch_dashboards", relation(mappingTable(".opensearch_dashboards")));
  }

  @Test
  public void testIdentifierAsIndexNameWithDotInTheMiddle() {
    assertEqual("source=log.2020.10.10", relation("log.2020.10.10"));
    assertEqual("source=log-7.10-2020.10.10", relation("log-7.10-2020.10.10"));
    assertEqual("describe log.2020.10.10", relation(mappingTable("log.2020.10.10")));
    assertEqual("describe log-7.10-2020.10.10", relation(mappingTable("log-7.10-2020.10.10")));
  }

  @Test
  public void testIdentifierAsIndexNameWithSlashInTheMiddle() {
    assertEqual("source=log-2020", relation("log-2020"));
    assertEqual("describe log-2020", relation(mappingTable("log-2020")));
  }

  @Test
  public void testIdentifierAsIndexNameContainStar() {
    assertEqual("source=log-2020-10-*", relation("log-2020-10-*"));
    assertEqual("describe log-2020-10-*", relation(mappingTable("log-2020-10-*")));
  }

  @Test
  public void testIdentifierAsIndexNameContainStarAndDots() {
    assertEqual("source=log-2020.10.*", relation("log-2020.10.*"));
    assertEqual("source=log-2020.*.01", relation("log-2020.*.01"));
    assertEqual("source=log-2020.*.*", relation("log-2020.*.*"));
    assertEqual("describe log-2020.10.*", relation(mappingTable("log-2020.10.*")));
    assertEqual("describe log-2020.*.01", relation(mappingTable("log-2020.*.01")));
    assertEqual("describe log-2020.*.*", relation(mappingTable("log-2020.*.*")));
  }

  @Test
  public void testIdentifierAsFieldNameStartWithAt() {
    assertEqual(
        "source=log-2020 | fields @timestamp",
        projectWithArg(relation("log-2020"), defaultFieldsArgs(), field("@timestamp")));
  }

  @Test
  public void testRareCommand() {
    assertEqual(
        "source=t | rare a",
        rareTopN(
            relation("t"),
            CommandType.RARE,
            exprList(argument("noOfResults", intLiteral(10))),
            emptyList(),
            field("a")));
  }

  @Test
  public void testRareCommandWithGroupBy() {
    assertEqual(
        "source=t | rare a by b",
        rareTopN(
            relation("t"),
            CommandType.RARE,
            exprList(argument("noOfResults", intLiteral(10))),
            exprList(field("b")),
            field("a")));
  }

  @Test
  public void testRareCommandWithMultipleFields() {
    assertEqual(
        "source=t | rare `a`, `b` by `c`",
        rareTopN(
            relation("t"),
            CommandType.RARE,
            exprList(argument("noOfResults", intLiteral(10))),
            exprList(field("c")),
            field("a"),
            field("b")));
  }

  @Test
  public void testTopCommandWithN() {
    assertEqual(
        "source=t | top 1 a",
        rareTopN(
            relation("t"),
            CommandType.TOP,
            exprList(argument("noOfResults", intLiteral(1))),
            emptyList(),
            field("a")));
  }

  @Test
  public void testTopCommandWithoutNAndGroupBy() {
    assertEqual(
        "source=t | top a",
        rareTopN(
            relation("t"),
            CommandType.TOP,
            exprList(argument("noOfResults", intLiteral(10))),
            emptyList(),
            field("a")));
  }

  @Test
  public void testTopCommandWithNAndGroupBy() {
    assertEqual(
        "source=t | top 1 a by b",
        rareTopN(
            relation("t"),
            CommandType.TOP,
            exprList(argument("noOfResults", intLiteral(1))),
            exprList(field("b")),
            field("a")));
  }

  @Test
  public void testTopCommandWithMultipleFields() {
    assertEqual(
        "source=t | top 1 `a`, `b` by `c`",
        rareTopN(
            relation("t"),
            CommandType.TOP,
            exprList(argument("noOfResults", intLiteral(1))),
            exprList(field("c")),
            field("a"),
            field("b")));
  }

  @Test
  public void testGrokCommand() {
    assertEqual(
        "source=t | grok raw \"pattern\"",
        parse(
            relation("t"),
            ParseMethod.GROK,
            field("raw"),
            stringLiteral("pattern"),
            ImmutableMap.of()));
  }

  @Test
  public void testParseCommand() {
    assertEqual(
        "source=t | parse raw \"pattern\"",
        parse(
            relation("t"),
            ParseMethod.REGEX,
            field("raw"),
            stringLiteral("pattern"),
            ImmutableMap.of()));
  }

  @Test
  public void testPatternsCommand() {
    assertEqual(
        "source=t | patterns new_field=\"custom_field\" " + "pattern=\"custom_pattern\" raw",
        parse(
            relation("t"),
            ParseMethod.PATTERNS,
            field("raw"),
            stringLiteral("custom_pattern"),
            ImmutableMap.<String, Literal>builder()
                .put("new_field", stringLiteral("custom_field"))
                .put("pattern", stringLiteral("custom_pattern"))
                .build()));
  }

  @Test
  public void testPatternsCommandWithoutArguments() {
    assertEqual(
        "source=t | patterns raw",
        parse(
            relation("t"),
            ParseMethod.PATTERNS,
            field("raw"),
            stringLiteral(""),
            ImmutableMap.of()));
  }

  @Test
  public void testKmeansCommand() {
    assertEqual(
        "source=t | kmeans centroids=3 iterations=2 distance_type='l1'",
        new Kmeans(
            relation("t"),
            ImmutableMap.<String, Literal>builder()
                .put("centroids", new Literal(3, DataType.INTEGER))
                .put("iterations", new Literal(2, DataType.INTEGER))
                .put("distance_type", new Literal("l1", DataType.STRING))
                .build()));
  }

  @Test
  public void testKmeansCommandWithoutParameter() {
    assertEqual("source=t | kmeans", new Kmeans(relation("t"), ImmutableMap.of()));
  }

  @Test
  public void testMLCommand() {
    assertEqual(
        "source=t | ml action='trainandpredict' "
            + "algorithm='kmeans' centroid=3 iteration=2 dist_type='l1'",
        new ML(
            relation("t"),
            ImmutableMap.<String, Literal>builder()
                .put("action", new Literal("trainandpredict", DataType.STRING))
                .put("algorithm", new Literal("kmeans", DataType.STRING))
                .put("centroid", new Literal(3, DataType.INTEGER))
                .put("iteration", new Literal(2, DataType.INTEGER))
                .put("dist_type", new Literal("l1", DataType.STRING))
                .build()));
  }

  @Test
  public void testFillNullCommandSameValue() {
    assertEqual(
        "source=t | fillnull with 0 in a, b, c",
        new FillNull(
            relation("t"),
            FillNull.ContainNullableFieldFill.ofSameValue(
                intLiteral(0),
                ImmutableList.<Field>builder()
                    .add(field("a"))
                    .add(field("b"))
                    .add(field("c"))
                    .build())));
  }

  @Test
  public void testFillNullCommandVariousValues() {
    assertEqual(
        "source=t | fillnull using a = 1, b = 2, c = 3",
        new FillNull(
            relation("t"),
            FillNull.ContainNullableFieldFill.ofVariousValue(
                ImmutableList.<FillNull.NullableFieldFill>builder()
                    .add(new FillNull.NullableFieldFill(field("a"), intLiteral(1)))
                    .add(new FillNull.NullableFieldFill(field("b"), intLiteral(2)))
                    .add(new FillNull.NullableFieldFill(field("c"), intLiteral(3)))
                    .build())));
  }

  public void testTrendline() {
    assertEqual(
        "source=t | trendline sma(5, test_field) as test_field_alias sma(1, test_field_2) as"
            + " test_field_alias_2",
        trendline(
            relation("t"),
            Optional.empty(),
            computation(5, field("test_field"), "test_field_alias", SMA),
            computation(1, field("test_field_2"), "test_field_alias_2", SMA)));
  }

  @Test
  public void testTrendlineSort() {
    assertEqual(
        "source=t | trendline sort test_field sma(5, test_field)",
        trendline(
            relation("t"),
            Optional.of(
                field(
                    "test_field",
                    argument("asc", booleanLiteral(true)),
                    argument("type", nullLiteral()))),
            computation(5, field("test_field"), "test_field_trendline", SMA)));
  }

  @Test
  public void testTrendlineSortDesc() {
    assertEqual(
        "source=t | trendline sort - test_field sma(5, test_field)",
        trendline(
            relation("t"),
            Optional.of(
                field(
                    "test_field",
                    argument("asc", booleanLiteral(false)),
                    argument("type", nullLiteral()))),
            computation(5, field("test_field"), "test_field_trendline", SMA)));
  }

  @Test
  public void testTrendlineSortAsc() {
    assertEqual(
        "source=t | trendline sort + test_field sma(5, test_field)",
        trendline(
            relation("t"),
            Optional.of(
                field(
                    "test_field",
                    argument("asc", booleanLiteral(true)),
                    argument("type", nullLiteral()))),
            computation(5, field("test_field"), "test_field_trendline", SMA)));
  }

  @Test
  public void testTrendlineNoAlias() {
    assertEqual(
        "source=t | trendline sma(5, test_field)",
        trendline(
            relation("t"),
            Optional.empty(),
            computation(5, field("test_field"), "test_field_trendline", SMA)));
  }

  @Test
  public void testTrendlineTooFewSamples() {
    assertThrows(SyntaxCheckException.class, () -> plan("source=t | trendline sma(0, test_field)"));
  }

  @Test
  public void testDescribeCommand() {
    assertEqual("describe t", relation(mappingTable("t")));
  }

  @Test
  public void testDescribeMatchAllCrossClusterSearchCommand() {
    assertEqual("describe *:t", relation(mappingTable("*:t")));
  }

  @Test
  public void testDescribeCommandWithMultipleIndices() {
    assertEqual("describe t,u", relation(mappingTable("t,u")));
  }

  @Test
  public void testDescribeCommandWithFullyQualifiedTableName() {
    assertEqual(
        "describe prometheus.http_metric",
        relation(qualifiedName("prometheus", mappingTable("http_metric"))));
    assertEqual(
        "describe prometheus.schema.http_metric",
        relation(qualifiedName("prometheus", "schema", mappingTable("http_metric"))));
  }

  @Test
  public void test_fitRCFADCommand_withoutDataFormat() {
    assertEqual(
        "source=t | AD shingle_size=10 time_decay=0.0001 time_field='timestamp' "
            + "anomaly_rate=0.1 anomaly_score_threshold=0.1 sample_size=256 "
            + "number_of_trees=256 time_zone='PST' output_after=256 "
            + "training_data_size=256",
        new AD(
            relation("t"),
            ImmutableMap.<String, Literal>builder()
                .put("anomaly_rate", new Literal(0.1, DataType.DOUBLE))
                .put("anomaly_score_threshold", new Literal(0.1, DataType.DOUBLE))
                .put("sample_size", new Literal(256, DataType.INTEGER))
                .put("number_of_trees", new Literal(256, DataType.INTEGER))
                .put("time_zone", new Literal("PST", DataType.STRING))
                .put("output_after", new Literal(256, DataType.INTEGER))
                .put("shingle_size", new Literal(10, DataType.INTEGER))
                .put("time_decay", new Literal(0.0001, DataType.DOUBLE))
                .put("time_field", new Literal("timestamp", DataType.STRING))
                .put("training_data_size", new Literal(256, DataType.INTEGER))
                .build()));
  }

  @Test
  public void test_fitRCFADCommand_withDataFormat() {
    assertEqual(
        "source=t | AD shingle_size=10 time_decay=0.0001 time_field='timestamp' "
            + "anomaly_rate=0.1 anomaly_score_threshold=0.1 sample_size=256 "
            + "number_of_trees=256 time_zone='PST' output_after=256 "
            + "training_data_size=256 date_format='HH:mm:ss yyyy-MM-dd'",
        new AD(
            relation("t"),
            ImmutableMap.<String, Literal>builder()
                .put("anomaly_rate", new Literal(0.1, DataType.DOUBLE))
                .put("anomaly_score_threshold", new Literal(0.1, DataType.DOUBLE))
                .put("sample_size", new Literal(256, DataType.INTEGER))
                .put("number_of_trees", new Literal(256, DataType.INTEGER))
                .put("date_format", new Literal("HH:mm:ss yyyy-MM-dd", DataType.STRING))
                .put("time_zone", new Literal("PST", DataType.STRING))
                .put("output_after", new Literal(256, DataType.INTEGER))
                .put("shingle_size", new Literal(10, DataType.INTEGER))
                .put("time_decay", new Literal(0.0001, DataType.DOUBLE))
                .put("time_field", new Literal("timestamp", DataType.STRING))
                .put("training_data_size", new Literal(256, DataType.INTEGER))
                .build()));
  }

  @Test
  public void test_batchRCFADCommand() {
    assertEqual("source=t | AD", new AD(relation("t"), ImmutableMap.of()));
  }

  @Test
  public void testShowDataSourcesCommand() {
    assertEqual("show datasources", relation(DATASOURCES_TABLE_NAME));
  }

  protected void assertEqual(String query, Node expectedPlan) {
    Node actualPlan = plan(query);
    assertEquals(expectedPlan, actualPlan);
  }

  protected void assertEqual(String query, String expected) {
    Node expectedPlan = plan(expected);
    assertEqual(query, expectedPlan);
  }

  private Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(new AstExpressionBuilder(), query);
    return astBuilder.visit(parser.parse(query));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertFalse;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.and;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.cast;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultFieldsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultSortFieldArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultStatsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.distinctAggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.equalTo;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.expand;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.flatten;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.in;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.intervalLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.let;
import static org.opensearch.sql.ast.dsl.AstDSL.longLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.not;
import static org.opensearch.sql.ast.dsl.AstDSL.nullLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.or;
import static org.opensearch.sql.ast.dsl.AstDSL.projectWithArg;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.sort;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;
import static org.opensearch.sql.ast.dsl.AstDSL.xor;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.common.utils.StringUtils;

public class AstExpressionBuilderTest extends AstBuilderTest {

  @Test
  public void testLogicalNotExpr() {
    assertEqual(
        "source=t not a=1", filter(relation("t"), not(compare("=", field("a"), intLiteral(1)))));
  }

  @Test
  public void testLogicalOrExpr() {
    assertEqual(
        "source=t a=1 or b=2",
        filter(
            relation("t"),
            or(compare("=", field("a"), intLiteral(1)), compare("=", field("b"), intLiteral(2)))));
  }

  @Test
  public void testLogicalAndExpr() {
    assertEqual(
        "source=t a=1 and b=2",
        filter(
            relation("t"),
            and(compare("=", field("a"), intLiteral(1)), compare("=", field("b"), intLiteral(2)))));
  }

  @Test
  public void testLogicalAndExprWithoutKeywordAnd() {
    assertEqual(
        "source=t a=1 b=2",
        filter(
            relation("t"),
            and(compare("=", field("a"), intLiteral(1)), compare("=", field("b"), intLiteral(2)))));
  }

  @Test
  public void testLogicalXorExpr() {
    assertEqual(
        "source=t a=1 xor b=2",
        filter(
            relation("t"),
            xor(compare("=", field("a"), intLiteral(1)), compare("=", field("b"), intLiteral(2)))));
  }

  @Test
  public void testLogicalLikeExpr() {
    assertEqual(
        "source=t like(a, '_a%b%c_d_')",
        filter(relation("t"), function("like", field("a"), stringLiteral("_a%b%c_d_"))));
  }

  @Test
  public void testBooleanIsNullFunction() {
    assertEqual("source=t isnull(a)", filter(relation("t"), function("is null", field("a"))));
    assertEqual("source=t ISNULL(a)", filter(relation("t"), function("is null", field("a"))));
  }

  @Test
  public void testBooleanIsNotNullFunction() {
    assertEqual(
        "source=t isnotnull(a)", filter(relation("t"), function("is not null", field("a"))));
    assertEqual(
        "source=t ISNOTNULL(a)", filter(relation("t"), function("is not null", field("a"))));
  }

  /** Todo. search operator should not include functionCall, need to change antlr. */
  @Ignore("search operator should not include functionCall, need to change antlr")
  public void testEvalExpr() {
    assertEqual(
        "source=t f=abs(a)",
        filter(relation("t"), equalTo(field("f"), function("abs", field("a")))));
  }

  @Test
  public void testEvalFunctionExpr() {
    assertEqual(
        "source=t | eval f=abs(a)",
        eval(relation("t"), let(field("f"), function("abs", field("a")))));
  }

  @Test
  public void testEvalFunctionExprNoArgs() {
    assertEqual("source=t | eval f=PI()", eval(relation("t"), let(field("f"), function("PI"))));
  }

  @Test
  public void testEvalIfFunctionExpr() {
    assertEqual(
        "source=t | eval f=if(true, 1, 0)",
        eval(
            relation("t"),
            let(field("f"), function("if", booleanLiteral(true), intLiteral(1), intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(1>2, 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if",
                    compare(">", intLiteral(1), intLiteral(2)),
                    intLiteral(1),
                    intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(1<=2, 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if",
                    compare("<=", intLiteral(1), intLiteral(2)),
                    intLiteral(1),
                    intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(1=2, 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if",
                    compare("=", intLiteral(1), intLiteral(2)),
                    intLiteral(1),
                    intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(1!=2, 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if",
                    compare("!=", intLiteral(1), intLiteral(2)),
                    intLiteral(1),
                    intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(isnull(a), 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function("if", function("is null", field("a")), intLiteral(1), intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(isnotnull(a), 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if", function("is not null", field("a")), intLiteral(1), intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(not 1>2, 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if",
                    not(compare(">", intLiteral(1), intLiteral(2))),
                    intLiteral(1),
                    intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(not a in (0, 1), 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if",
                    not(in(field("a"), intLiteral(0), intLiteral(1))),
                    intLiteral(1),
                    intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(not a in (0, 1) OR isnull(a), 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if",
                    or(
                        not(in(field("a"), intLiteral(0), intLiteral(1))),
                        function("is null", field("a"))),
                    intLiteral(1),
                    intLiteral(0)))));
    assertEqual(
        "source=t | eval f=if(like(a, '_a%b%c_d_'), 1, 0)",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "if",
                    function("like", field("a"), stringLiteral("_a%b%c_d_")),
                    intLiteral(1),
                    intLiteral(0)))));
  }

  @Test
  public void testExpandExpr() {
    String fieldName = "field_name";
    assertEqual(
        StringUtils.format("source=t | expand %s", fieldName),
        expand(relation("t"), field(fieldName)));
  }

  @Test
  public void testFlattenExpr() {
    String fieldName = "field_name";
    assertEqual(
        StringUtils.format("source=t | flatten %s", fieldName),
        flatten(relation("t"), field(fieldName)));
  }

  @Test
  public void testPositionFunctionExpr() {
    assertEqual(
        "source=t | eval f=position('substr' IN 'str')",
        eval(
            relation("t"),
            let(field("f"), function("position", stringLiteral("substr"), stringLiteral("str")))));
  }

  @Test
  public void testEvalBinaryOperationExpr() {
    assertEqual(
        "source=t | eval f=a+b",
        eval(relation("t"), let(field("f"), function("+", field("a"), field("b")))));
    assertEqual(
        "source=t | eval f=(a+b)",
        eval(relation("t"), let(field("f"), function("+", field("a"), field("b")))));
  }

  @Test
  public void testLiteralValueBinaryOperationExpr() {
    assertEqual(
        "source=t | eval f=3+2",
        eval(relation("t"), let(field("f"), function("+", intLiteral(3), intLiteral(2)))));
  }

  @Test
  public void testBinaryOperationExprWithParentheses() {
    assertEqual(
        "source = t | where a = (1 + 2) * 3",
        filter(
            relation("t"),
            compare(
                "=",
                field("a"),
                function("*", function("+", intLiteral(1), intLiteral(2)), intLiteral(3)))));
  }

  @Test
  public void testBinaryOperationExprPrecedence() {
    assertEqual(
        "source = t | where a = 1 + 2 * 3",
        filter(
            relation("t"),
            compare(
                "=",
                field("a"),
                function("+", intLiteral(1), function("*", intLiteral(2), intLiteral(3))))));
  }

  @Test
  public void testCompareExpr() {
    assertEqual(
        "source=t a='b'", filter(relation("t"), compare("=", field("a"), stringLiteral("b"))));
  }

  @Test
  public void testCompareFieldsExpr() {
    assertEqual("source=t a>b", filter(relation("t"), compare(">", field("a"), field("b"))));
  }

  @Test
  public void testInExpr() {
    assertEqual(
        "source=t f in (1, 2, 3)",
        filter(relation("t"), in(field("f"), intLiteral(1), intLiteral(2), intLiteral(3))));
  }

  @Test
  public void testFieldExpr() {
    assertEqual("source=t | sort + f", sort(relation("t"), field("f", defaultSortFieldArgs())));
  }

  @Test
  public void testSortFieldWithMinusKeyword() {
    assertEqual(
        "source=t | sort - f",
        sort(
            relation("t"),
            field("f", argument("asc", booleanLiteral(false)), argument("type", nullLiteral()))));
  }

  @Test
  public void testSortFieldWithBackticks() {
    assertEqual("source=t | sort `f`", sort(relation("t"), field("f", defaultSortFieldArgs())));
  }

  @Test
  public void testSortFieldWithAutoKeyword() {
    assertEqual(
        "source=t | sort auto(f)",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(true)),
                argument("type", stringLiteral("auto")))));
  }

  @Test
  public void testSortFieldWithIpKeyword() {
    assertEqual(
        "source=t | sort ip(f)",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(true)),
                argument("type", stringLiteral("ip")))));
  }

  @Test
  public void testSortFieldWithNumKeyword() {
    assertEqual(
        "source=t | sort num(f)",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(true)),
                argument("type", stringLiteral("num")))));
  }

  @Test
  public void testSortFieldWithStrKeyword() {
    assertEqual(
        "source=t | sort str(f)",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(true)),
                argument("type", stringLiteral("str")))));
  }

  @Test
  public void testAggFuncCallExpr() {
    assertEqual(
        "source=t | stats avg(a) by b",
        agg(
            relation("t"),
            exprList(alias("avg(a)", aggregate("avg", field("a")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            defaultStatsArgs()));
  }

  @Test
  public void testVarAggregationShouldPass() {
    assertEqual(
        "source=t | stats var_samp(a) by b",
        agg(
            relation("t"),
            exprList(alias("var_samp(a)", aggregate("var_samp", field("a")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            defaultStatsArgs()));
  }

  @Test
  public void testVarpAggregationShouldPass() {
    assertEqual(
        "source=t | stats var_pop(a) by b",
        agg(
            relation("t"),
            exprList(alias("var_pop(a)", aggregate("var_pop", field("a")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            defaultStatsArgs()));
  }

  @Test
  public void testStdDevAggregationShouldPass() {
    assertEqual(
        "source=t | stats stddev_samp(a) by b",
        agg(
            relation("t"),
            exprList(alias("stddev_samp(a)", aggregate("stddev_samp", field("a")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            defaultStatsArgs()));
  }

  @Test
  public void testStdDevPAggregationShouldPass() {
    assertEqual(
        "source=t | stats stddev_pop(a) by b",
        agg(
            relation("t"),
            exprList(alias("stddev_pop(a)", aggregate("stddev_pop", field("a")))),
            emptyList(),
            exprList(alias("b", field("b"))),
            defaultStatsArgs()));
  }

  @Test
  public void testPercentileAggFuncExpr() {
    assertEqual(
        "source=t | stats percentile(a, 1)",
        agg(
            relation("t"),
            exprList(
                alias(
                    "percentile(a, 1)",
                    aggregate("percentile", field("a"), unresolvedArg("percent", intLiteral(1))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
    assertEqual(
        "source=t | stats percentile(a, 1.0)",
        agg(
            relation("t"),
            exprList(
                alias(
                    "percentile(a, 1.0)",
                    aggregate(
                        "percentile", field("a"), unresolvedArg("percent", doubleLiteral(1D))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
    assertEqual(
        "source=t | stats percentile(a, 1.0, 100)",
        agg(
            relation("t"),
            exprList(
                alias(
                    "percentile(a, 1.0, 100)",
                    aggregate(
                        "percentile",
                        field("a"),
                        unresolvedArg("percent", doubleLiteral(1D)),
                        unresolvedArg("compression", intLiteral(100))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void testCountFuncCallExpr() {
    assertEqual(
        "source=t | stats count() by b",
        agg(
            relation("t"),
            exprList(alias("count()", aggregate("count", AllFields.of()))),
            emptyList(),
            exprList(alias("b", field("b"))),
            defaultStatsArgs()));
  }

  @Test
  public void testDistinctCount() {
    assertEqual(
        "source=t | stats distinct_count(a)",
        agg(
            relation("t"),
            exprList(alias("distinct_count(a)", distinctAggregate("count", field("a")))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void testTakeAggregationNoArgsShouldPass() {
    assertEqual(
        "source=t | stats take(a)",
        agg(
            relation("t"),
            exprList(
                alias(
                    "take(a)",
                    aggregate("take", field("a"), unresolvedArg("size", intLiteral(10))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void testTakeAggregationWithArgsShouldPass() {
    assertEqual(
        "source=t | stats take(a, 5)",
        agg(
            relation("t"),
            exprList(
                alias(
                    "take(a, 5)",
                    aggregate("take", field("a"), unresolvedArg("size", intLiteral(5))))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void testEvalFuncCallExpr() {
    assertEqual(
        "source=t | eval f=abs(a)",
        eval(relation("t"), let(field("f"), function("abs", field("a")))));
  }

  @Test
  public void testDataTypeFuncCall() {
    assertEqual(
        "source=t | eval f=cast(1 as string)",
        eval(relation("t"), let(field("f"), cast(intLiteral(1), stringLiteral("string")))));
  }

  @Test
  public void testNestedFieldName() {
    assertEqual(
        "source=t | fields field0.field1.field2",
        projectWithArg(
            relation("t"),
            defaultFieldsArgs(),
            field(qualifiedName("field0", "field1", "field2"))));
  }

  @Test
  public void testFieldNameWithSpecialChars() {
    assertEqual(
        "source=t | fields `field-0`",
        projectWithArg(relation("t"), defaultFieldsArgs(), field(qualifiedName("field-0"))));
  }

  @Test
  public void testNestedFieldNameWithSpecialChars() {
    assertEqual(
        "source=t | fields `field-0`.`field#1`.`field*2`",
        projectWithArg(
            relation("t"),
            defaultFieldsArgs(),
            field(qualifiedName("field-0", "field#1", "field*2"))));
  }

  @Test
  public void testStringLiteralExpr() {
    assertEqual(
        "source=t a=\"string\"",
        filter(relation("t"), compare("=", field("a"), stringLiteral("string"))));
  }

  @Test
  public void testIntegerLiteralExpr() {
    assertEqual(
        "source=t a=1 b=-1",
        filter(
            relation("t"),
            and(
                compare("=", field("a"), intLiteral(1)),
                compare("=", field("b"), intLiteral(-1)))));
  }

  @Test
  public void testLongLiteralExpr() {
    assertEqual(
        "source=t a=1234567890123 b=-1234567890123",
        filter(
            relation("t"),
            and(
                compare("=", field("a"), longLiteral(1234567890123L)),
                compare("=", field("b"), longLiteral(-1234567890123L)))));
  }

  @Test
  public void testDoubleLiteralExpr() {
    assertEqual(
        "source=t b=0.1", filter(relation("t"), compare("=", field("b"), doubleLiteral(0.1))));
  }

  @Test
  public void testBooleanLiteralExpr() {
    assertEqual(
        "source=t a=true", filter(relation("t"), compare("=", field("a"), booleanLiteral(true))));
  }

  @Test
  public void testIntervalLiteralExpr() {
    assertEqual(
        "source=t a = interval 1 day",
        filter(
            relation("t"), compare("=", field("a"), intervalLiteral(1, DataType.INTEGER, "day"))));
  }

  @Test
  public void testKeywordsAsIdentifiers() {
    assertEqual("source=timestamp", relation("timestamp"));

    assertEqual(
        "source=t | fields timestamp",
        projectWithArg(relation("t"), defaultFieldsArgs(), field("timestamp")));
  }

  @Test
  public void canBuildKeywordsAsIdentInQualifiedName() {
    assertEqual(
        "source=test | fields timestamp",
        projectWithArg(relation("test"), defaultFieldsArgs(), field("timestamp")));
  }

  @Test
  public void canBuildMetaDataFieldAsQualifiedName() {
    assertEqual(
        "source=test | fields _id, _index, _sort, _maxscore",
        projectWithArg(
            relation("test"),
            defaultFieldsArgs(),
            field("_id"),
            field("_index"),
            field("_sort"),
            field("_maxscore")));
  }

  @Test
  public void canBuildNonMetaDataFieldAsQualifiedName() {
    assertEqual(
        "source=test | fields id, __id, _routing, ___field",
        projectWithArg(
            relation("test"),
            defaultFieldsArgs(),
            field("id"),
            field("__id"),
            field("_routing"),
            field("___field")));
  }

  @Test
  public void canBuildMatchRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where match('message', 'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "match",
                unresolvedArg("field", qualifiedName("message")),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword")))));
  }

  @Test
  public void canBuildMulti_matchRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where multi_match(['field1', 'field2' ^ 3.2],"
            + "'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "multi_match",
                unresolvedArg(
                    "fields",
                    new RelevanceFieldList(ImmutableMap.of("field1", 1.F, "field2", 3.2F))),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword")))));
  }

  @Test
  public void canBuildSimple_query_stringRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where simple_query_string(['field1', 'field2' ^ 3.2],"
            + "'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "simple_query_string",
                unresolvedArg(
                    "fields",
                    new RelevanceFieldList(ImmutableMap.of("field1", 1.F, "field2", 3.2F))),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword")))));
  }

  @Test
  public void canBuildQuery_stringRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where query_string(['field1', 'field2' ^ 3.2],"
            + "'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "query_string",
                unresolvedArg(
                    "fields",
                    new RelevanceFieldList(ImmutableMap.of("field1", 1.F, "field2", 3.2F))),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword")))));
  }

  @Test
  public void functionNameCanBeUsedAsIdentifier() {
    assertFunctionNameCouldBeId(
        "AVG | COUNT | SUM | MIN | MAX | VAR_SAMP | VAR_POP | STDDEV_SAMP | STDDEV_POP |"
            + " PERCENTILE");
    assertFunctionNameCouldBeId(
        "CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP | LOCALTIME | LOCALTIMESTAMP | "
            + "UTC_TIMESTAMP | UTC_DATE | UTC_TIME | CURDATE | CURTIME | NOW");
    assertFunctionNameCouldBeId(
        "ADDDATE | CONVERT_TZ | DATE | DATE_ADD | DATE_FORMAT | DATE_SUB "
            + "| DATETIME | DAY | DAYNAME | DAYOFMONTH "
            + "| DAYOFWEEK | DAYOFYEAR | FROM_DAYS | FROM_UNIXTIME | HOUR | MAKEDATE | MAKETIME "
            + "| MICROSECOND | MINUTE | MONTH | MONTHNAME "
            + "| PERIOD_ADD | PERIOD_DIFF | QUARTER | SECOND | SUBDATE | SYSDATE | TIME "
            + "| TIME_TO_SEC | TIMESTAMP | TO_DAYS | UNIX_TIMESTAMP | WEEK | YEAR");
    assertFunctionNameCouldBeId(
        "SUBSTR | SUBSTRING | TRIM | LTRIM | RTRIM | LOWER | UPPER | CONCAT | CONCAT_WS | LENGTH "
            + "| STRCMP | RIGHT | LEFT | ASCII | LOCATE | REPLACE");
    assertFunctionNameCouldBeId(
        "ABS | CEIL | CEILING | CONV | CRC32 | E | EXP | FLOOR | LN | LOG"
            + " | LOG10 | LOG2 | MOD | PI |POW | POWER | RAND | ROUND | SIGN | SQRT | TRUNCATE "
            + "| ACOS | ASIN | ATAN | ATAN2 | COS | COT | DEGREES | RADIANS | SIN | TAN");
    assertFunctionNameCouldBeId(
        "SEARCH | DESCRIBE | SHOW | FROM | WHERE | FIELDS | RENAME | STATS "
            + "| DEDUP | SORT | EVAL | HEAD | TOP | RARE | PARSE | METHOD | REGEX | PUNCT | GROK "
            + "| PATTERN | PATTERNS | NEW_FIELD | KMEANS | AD | ML | SOURCE | INDEX | D | DESC "
            + "| DATASOURCES");
  }

  void assertFunctionNameCouldBeId(String antlrFunctionName) {
    List<String> functionList =
        Arrays.stream(antlrFunctionName.split("\\|"))
            .map(String::stripLeading)
            .map(String::stripTrailing)
            .collect(Collectors.toList());

    assertFalse(functionList.isEmpty());
    for (String functionName : functionList) {
      assertEqual(
          StringUtils.format("source=t | fields %s", functionName),
          projectWithArg(relation("t"), defaultFieldsArgs(), field(qualifiedName(functionName))));
    }
  }

  // https://github.com/opensearch-project/sql/issues/1318
  @Test
  public void indexCanBeId() {
    assertEqual(
        "source = index | stats count() by index",
        agg(
            relation("index"),
            exprList(alias("count()", aggregate("count", AllFields.of()))),
            emptyList(),
            exprList(alias("index", field("index"))),
            defaultStatsArgs()));
  }

  @Test
  public void testExtractFunctionExpr() {
    assertEqual(
        "source=t | eval f=extract(day from '2001-05-07 10:11:12')",
        eval(
            relation("t"),
            let(
                field("f"),
                function("extract", stringLiteral("day"), stringLiteral("2001-05-07 10:11:12")))));
  }

  @Test
  public void testGet_FormatFunctionExpr() {
    assertEqual(
        "source=t | eval f=get_format(DATE,'USA')",
        eval(
            relation("t"),
            let(field("f"), function("get_format", stringLiteral("DATE"), stringLiteral("USA")))));
  }

  @Test
  public void testTimeStampAddFunctionExpr() {
    assertEqual(
        "source=t | eval f=timestampadd(YEAR, 15, '2001-03-06 00:00:00')",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "timestampadd",
                    stringLiteral("YEAR"),
                    intLiteral(15),
                    stringLiteral("2001-03-06 00:00:00")))));
  }

  @Test
  public void testTimeStampDiffFunctionExpr() {
    assertEqual(
        "source=t | eval f=timestampdiff(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')",
        eval(
            relation("t"),
            let(
                field("f"),
                function(
                    "timestampdiff",
                    stringLiteral("YEAR"),
                    stringLiteral("1997-01-01 00:00:00"),
                    stringLiteral("2001-03-06 00:00:00")))));
  }
}

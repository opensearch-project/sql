/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.parser;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.and;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultFieldsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultSortFieldArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultStatsArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.distinctAggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.equalTo;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
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

import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.DataType;

public class AstExpressionBuilderTest extends AstBuilderTest {

  @Test
  public void testLogicalNotExpr() {
    assertEqual("source=t not a=1",
        filter(
            relation("t"),
            not(
                compare("=", field("a"), intLiteral(1))
            )
        ));
  }

  @Test
  public void testLogicalOrExpr() {
    assertEqual("source=t a=1 or b=2",
        filter(
            relation("t"),
            or(
                compare("=", field("a"), intLiteral(1)),
                compare("=", field("b"), intLiteral(2))
            )
        ));
  }

  @Test
  public void testLogicalAndExpr() {
    assertEqual("source=t a=1 and b=2",
        filter(
            relation("t"),
            and(
                compare("=", field("a"), intLiteral(1)),
                compare("=", field("b"), intLiteral(2))
            )
        ));
  }

  @Test
  public void testLogicalAndExprWithoutKeywordAnd() {
    assertEqual("source=t a=1 b=2",
        filter(
            relation("t"),
            and(
                compare("=", field("a"), intLiteral(1)),
                compare("=", field("b"), intLiteral(2))
            )
        ));
  }

  @Test
  public void testLogicalXorExpr() {
    assertEqual("source=t a=1 xor b=2",
        filter(
            relation("t"),
            xor(
                compare("=", field("a"), intLiteral(1)),
                compare("=", field("b"), intLiteral(2))
            )
        ));
  }

  @Test
  public void testLogicalLikeExpr() {
    assertEqual("source=t like(a, '_a%b%c_d_')",
        filter(
            relation("t"),
            function("like", field("a"), stringLiteral("_a%b%c_d_"))
        ));
  }

  @Test
  public void testBooleanIsNullFunction() {
    assertEqual("source=t isnull(a)",
        filter(
            relation("t"),
            function("is null", field("a"))
        ));
  }

  @Test
  public void testBooleanIsNotNullFunction() {
    assertEqual("source=t isnotnull(a)",
        filter(
            relation("t"),
            function("is not null", field("a"))
        ));
  }

  /**
   * Todo. search operator should not include functionCall, need to change antlr.
   */
  @Ignore("search operator should not include functionCall, need to change antlr")
  public void testEvalExpr() {
    assertEqual("source=t f=abs(a)",
        filter(
            relation("t"),
            equalTo(
                field("f"),
                function("abs", field("a"))
            )
        ));
  }

  @Test
  public void testEvalFunctionExpr() {
    assertEqual("source=t | eval f=abs(a)",
        eval(
            relation("t"),
            let(
                field("f"),
                function("abs", field("a"))
            )
        ));
  }

  @Test
  public void testEvalBinaryOperationExpr() {
    assertEqual("source=t | eval f=a+b",
        eval(
            relation("t"),
            let(
                field("f"),
                function("+", field("a"), field("b"))
            )
        ));
    assertEqual("source=t | eval f=(a+b)",
        eval(
            relation("t"),
            let(
                field("f"),
                function("+", field("a"), field("b"))
            )
        ));
  }

  @Test
  public void testLiteralValueBinaryOperationExpr() {
    assertEqual("source=t | eval f=3+2",
        eval(
            relation("t"),
            let(
                field("f"),
                function("+", intLiteral(3), intLiteral(2))
            )
        ));
  }

  @Test
  public void testCompareExpr() {
    assertEqual("source=t a='b'",
        filter(
            relation("t"),
            compare("=", field("a"), stringLiteral("b"))
        ));
  }

  @Test
  public void testCompareFieldsExpr() {
    assertEqual("source=t a>b",
        filter(
            relation("t"),
            compare(">", field("a"), field("b"))
        ));
  }

  @Test
  public void testInExpr() {
    assertEqual("source=t f in (1, 2, 3)",
        filter(
            relation("t"),
            in(
                field("f"),
                intLiteral(1), intLiteral(2), intLiteral(3))
        ));
  }

  @Test
  public void testFieldExpr() {
    assertEqual("source=t | sort + f",
        sort(
            relation("t"),
            field("f", defaultSortFieldArgs())
        ));
  }

  @Test
  public void testSortFieldWithMinusKeyword() {
    assertEqual("source=t | sort - f",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(false)),
                argument("type", nullLiteral())
            )
        ));
  }

  @Test
  public void testSortFieldWithAutoKeyword() {
    assertEqual("source=t | sort auto(f)",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(true)),
                argument("type", stringLiteral("auto"))
            )
        ));
  }

  @Test
  public void testSortFieldWithIpKeyword() {
    assertEqual("source=t | sort ip(f)",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(true)),
                argument("type", stringLiteral("ip"))
            )
        ));
  }

  @Test
  public void testSortFieldWithNumKeyword() {
    assertEqual("source=t | sort num(f)",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(true)),
                argument("type", stringLiteral("num"))
            )
        ));
  }

  @Test
  public void testSortFieldWithStrKeyword() {
    assertEqual("source=t | sort str(f)",
        sort(
            relation("t"),
            field(
                "f",
                argument("asc", booleanLiteral(true)),
                argument("type", stringLiteral("str"))
            )
        ));
  }

  @Test
  public void testAggFuncCallExpr() {
    assertEqual("source=t | stats avg(a) by b",
        agg(
            relation("t"),
            exprList(
                alias(
                    "avg(a)",
                    aggregate("avg", field("a"))
                )
            ),
            emptyList(),
            exprList(
                alias(
                    "b",
                    field("b")
                )),
            defaultStatsArgs()
        ));
  }

  @Test
  public void testVarAggregationShouldPass() {
    assertEqual("source=t | stats var_samp(a) by b",
        agg(
            relation("t"),
            exprList(
                alias(
                    "var_samp(a)",
                    aggregate("var_samp", field("a"))
                )
            ),
            emptyList(),
            exprList(
                alias(
                    "b",
                    field("b")
                )),
            defaultStatsArgs()
        ));
  }

  @Test
  public void testVarpAggregationShouldPass() {
    assertEqual("source=t | stats var_pop(a) by b",
        agg(
            relation("t"),
            exprList(
                alias(
                    "var_pop(a)",
                    aggregate("var_pop", field("a"))
                )
            ),
            emptyList(),
            exprList(
                alias(
                    "b",
                    field("b")
                )),
            defaultStatsArgs()
        ));
  }

  @Test
  public void testStdDevAggregationShouldPass() {
    assertEqual("source=t | stats stddev_samp(a) by b",
        agg(
            relation("t"),
            exprList(
                alias(
                    "stddev_samp(a)",
                    aggregate("stddev_samp", field("a"))
                )
            ),
            emptyList(),
            exprList(
                alias(
                    "b",
                    field("b")
                )),
            defaultStatsArgs()
        ));
  }

  @Test
  public void testStdDevPAggregationShouldPass() {
    assertEqual("source=t | stats stddev_pop(a) by b",
        agg(
            relation("t"),
            exprList(
                alias(
                    "stddev_pop(a)",
                    aggregate("stddev_pop", field("a"))
                )
            ),
            emptyList(),
            exprList(
                alias(
                    "b",
                    field("b")
                )),
            defaultStatsArgs()
        ));
  }

  @Test
  public void testPercentileAggFuncExpr() {
    assertEqual("source=t | stats percentile<1>(a)",
        agg(
            relation("t"),
            exprList(
                alias("percentile<1>(a)",
                    aggregate(
                        "percentile",
                        field("a"),
                        argument("rank", intLiteral(1))
                    )
                )
            ),
            emptyList(),
            emptyList(),
            defaultStatsArgs()
        ));
  }

  @Test
  public void testCountFuncCallExpr() {
    assertEqual("source=t | stats count() by b",
        agg(
            relation("t"),
            exprList(
                alias(
                    "count()",
                    aggregate("count", AllFields.of())
                )
            ),
            emptyList(),
            exprList(
                alias(
                    "b",
                    field("b")
                )),
            defaultStatsArgs()
        ));
  }

  @Test
  public void testDistinctCount() {
    assertEqual("source=t | stats distinct_count(a)",
        agg(
            relation("t"),
            exprList(
                alias("distinct_count(a)",
                    distinctAggregate("count", field("a")))),
            emptyList(),
            emptyList(),
            defaultStatsArgs()));
  }

  @Test
  public void testEvalFuncCallExpr() {
    assertEqual("source=t | eval f=abs(a)",
        eval(
            relation("t"),
            let(
                field("f"),
                function("abs", field("a"))
            )
        ));
  }

  @Ignore("Nested field is not supported in backend yet")
  @Test
  public void testNestedFieldName() {
    assertEqual("source=t | fields field0.field1.field2",
        projectWithArg(
            relation("t"),
            defaultFieldsArgs(),
            field(
                qualifiedName("field0", "field1", "field2")
            )
        ));
  }

  @Test
  public void testFieldNameWithSpecialChars() {
    assertEqual("source=t | fields `field-0`",
        projectWithArg(
            relation("t"),
            defaultFieldsArgs(),
            field(
                qualifiedName("field-0")
            )
        ));
  }

  @Ignore("Nested field is not supported in backend yet")
  @Test
  public void testNestedFieldNameWithSpecialChars() {
    assertEqual("source=t | fields `field-0`.`field#1`.`field*2`",
        projectWithArg(
            relation("t"),
            defaultFieldsArgs(),
            field(
                qualifiedName("field-0", "field#1", "field*2")
            )
        ));
  }

  @Test
  public void testStringLiteralExpr() {
    assertEqual("source=t a=\"string\"",
        filter(
            relation("t"),
            compare(
                "=",
                field("a"),
                stringLiteral("string")
            )
        ));
  }

  @Test
  public void testIntegerLiteralExpr() {
    assertEqual("source=t a=1 b=-1",
        filter(
            relation("t"),
            and(
                compare(
                    "=",
                    field("a"),
                    intLiteral(1)
                ),
                compare(
                    "=",
                    field("b"),
                    intLiteral(-1)
                )
            )
        ));
  }

  @Test
  public void testLongLiteralExpr() {
    assertEqual("source=t a=1234567890123 b=-1234567890123",
        filter(
            relation("t"),
            and(
                compare(
                    "=",
                    field("a"),
                    longLiteral(1234567890123L)
                ),
                compare(
                    "=",
                    field("b"),
                    longLiteral(-1234567890123L)
                )
            )
        ));
  }

  @Test
  public void testDoubleLiteralExpr() {
    assertEqual("source=t b=0.1",
        filter(
            relation("t"),
            compare(
                "=",
                field("b"),
                doubleLiteral(0.1)
            )
        ));
  }

  @Test
  public void testBooleanLiteralExpr() {
    assertEqual("source=t a=true",
        filter(
            relation("t"),
            compare(
                "=",
                field("a"),
                booleanLiteral(true)
            )
        ));
  }

  @Test
  public void testIntervalLiteralExpr() {
    assertEqual(
        "source=t a = interval 1 day",
        filter(
            relation("t"),
            compare(
                "=",
                field("a"),
                intervalLiteral(1, DataType.INTEGER, "day")
            )
        ));
  }

  @Test
  public void testKeywordsAsIdentifiers() {
    assertEqual(
        "source=timestamp",
        relation("timestamp")
    );

    assertEqual(
        "source=t | fields timestamp",
        projectWithArg(
            relation("t"),
            defaultFieldsArgs(),
            field("timestamp")
        )
    );
  }

  @Test
  public void canBuildKeywordsAsIdentInQualifiedName() {
    assertEqual(
        "source=test | fields timestamp",
        projectWithArg(
            relation("test"),
            defaultFieldsArgs(),
            field("timestamp")
        )
    );
  }

  @Test
  public void canBuildRelevanceFunctionWithArguments() {
    assertEqual(
        "source=test | where match(message, 'test query', analyzer='keyword')",
        filter(
            relation("test"),
            function(
                "match",
                unresolvedArg("field", stringLiteral("message")),
                unresolvedArg("query", stringLiteral("test query")),
                unresolvedArg("analyzer", stringLiteral("keyword"))
            )
        )
    );
  }
}

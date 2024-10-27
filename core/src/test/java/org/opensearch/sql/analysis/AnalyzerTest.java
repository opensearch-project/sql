/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.analysis.NestedAnalyzer.isNestedFunction;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.computation;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.filteredAggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.nestedAllTupleFields;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.span;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;
import static org.opensearch.sql.ast.tree.Sort.NullOrder;
import static org.opensearch.sql.ast.tree.Sort.SortOption;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.utils.MLCommonsConstants.ACTION;
import static org.opensearch.sql.utils.MLCommonsConstants.ALGO;
import static org.opensearch.sql.utils.MLCommonsConstants.ASYNC;
import static org.opensearch.sql.utils.MLCommonsConstants.CLUSTERID;
import static org.opensearch.sql.utils.MLCommonsConstants.KMEANS;
import static org.opensearch.sql.utils.MLCommonsConstants.MODELID;
import static org.opensearch.sql.utils.MLCommonsConstants.PREDICT;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_ANOMALOUS;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_ANOMALY_GRADE;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_SCORE;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_TIME_FIELD;
import static org.opensearch.sql.utils.MLCommonsConstants.STATUS;
import static org.opensearch.sql.utils.MLCommonsConstants.TASKID;
import static org.opensearch.sql.utils.MLCommonsConstants.TRAIN;
import static org.opensearch.sql.utils.SystemIndexUtils.DATASOURCES_TABLE_NAME;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.ScoreFunction;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.CloseCursor;
import org.opensearch.sql.ast.tree.FetchCursor;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.ML;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.planner.logical.LogicalAD;
import org.opensearch.sql.planner.logical.LogicalCloseCursor;
import org.opensearch.sql.planner.logical.LogicalFetchCursor;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalMLCommons;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.datasource.DataSourceTable;

class AnalyzerTest extends AnalyzerTestBase {

  @Test
  public void filter_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation("schema"),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_reserved_qualifiedName() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.equal(DSL.ref("_test", STRING), DSL.literal(stringValue("value")))),
        AstDSL.filter(
            AstDSL.relation("schema"),
            AstDSL.equalTo(AstDSL.qualifiedName("_test"), AstDSL.stringLiteral("value"))));
  }

  @Test
  public void filter_relation_with_invalid_qualifiedName_SemanticCheckException() {
    UnresolvedPlan invalidFieldPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            AstDSL.equalTo(AstDSL.qualifiedName("_invalid"), AstDSL.stringLiteral("value")));

    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> analyze(invalidFieldPlan));
    assertEquals(
        "can't resolve Symbol(namespace=FIELD_NAME, name=_invalid) in type env",
        exception.getMessage());
  }

  @Test
  public void filter_relation_with_invalid_qualifiedName_ExpressionEvaluationException() {
    UnresolvedPlan typeMismatchPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            AstDSL.equalTo(AstDSL.qualifiedName("_test"), AstDSL.intLiteral(1)));

    ExpressionEvaluationException exception =
        assertThrows(ExpressionEvaluationException.class, () -> analyze(typeMismatchPlan));
    assertEquals(
        "= function expected {[BYTE,BYTE],[SHORT,SHORT],[INTEGER,INTEGER],[LONG,LONG],"
            + "[FLOAT,FLOAT],[DOUBLE,DOUBLE],[STRING,STRING],[BOOLEAN,BOOLEAN],[DATE,DATE],"
            + "[TIME,TIME],[TIMESTAMP,TIMESTAMP],[INTERVAL,INTERVAL],"
            + "[STRUCT,STRUCT],[ARRAY,ARRAY]}, but got [STRING,INTEGER]",
        exception.getMessage());
  }

  @Test
  public void filter_relation_with_alias() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation("schema", "alias"),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_datasource() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("http_total_requests", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("prometheus", "http_total_requests")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_escaped_datasource() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("prometheus.http_total_requests", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("prometheus.http_total_requests")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_information_schema_and_prom_datasource() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("tables", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("prometheus", "information_schema", "tables")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_default_schema_and_prom_datasource() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("tables", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("prometheus", "default", "tables")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_information_schema_and_os_datasource() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("tables", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(
                AstDSL.qualifiedName(DEFAULT_DATASOURCE_NAME, "information_schema", "tables")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_information_schema() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("tables.test", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("information_schema", "tables", "test")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_non_existing_datasource() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("test.http_total_requests", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("test", "http_total_requests")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_non_existing_datasource_with_three_parts() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("test.nonexisting_schema.http_total_requests", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(
                AstDSL.qualifiedName("test", "nonexisting_schema", "http_total_requests")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_multiple_tables() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("test.1,test.2", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(Arrays.asList("test.1", "test.2")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void analyze_filter_visit_score_function() {
    UnresolvedPlan unresolvedPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            new ScoreFunction(
                AstDSL.function(
                    "match_phrase_prefix",
                    AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                    AstDSL.unresolvedArg("query", stringLiteral("search query")),
                    AstDSL.unresolvedArg("boost", stringLiteral("3"))),
                AstDSL.doubleLiteral(1.0)));
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.match_phrase_prefix(
                DSL.namedArgument("field", "field_value1"),
                DSL.namedArgument("query", "search query"),
                DSL.namedArgument("boost", "3.0"))),
        unresolvedPlan);

    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunctions.OpenSearchFunction relevanceQuery =
        (OpenSearchFunctions.OpenSearchFunction) ((LogicalFilter) logicalPlan).getCondition();
    assertEquals(true, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_without_score_function() {
    UnresolvedPlan unresolvedPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            AstDSL.function(
                "match_phrase_prefix",
                AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                AstDSL.unresolvedArg("query", stringLiteral("search query")),
                AstDSL.unresolvedArg("boost", stringLiteral("3"))));
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.match_phrase_prefix(
                DSL.namedArgument("field", "field_value1"),
                DSL.namedArgument("query", "search query"),
                DSL.namedArgument("boost", "3"))),
        unresolvedPlan);

    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunctions.OpenSearchFunction relevanceQuery =
        (OpenSearchFunctions.OpenSearchFunction) ((LogicalFilter) logicalPlan).getCondition();
    assertEquals(false, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_score_function_with_double_boost() {
    UnresolvedPlan unresolvedPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            new ScoreFunction(
                AstDSL.function(
                    "match_phrase_prefix",
                    AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                    AstDSL.unresolvedArg("query", stringLiteral("search query")),
                    AstDSL.unresolvedArg("slop", stringLiteral("3"))),
                new Literal(3.0, DataType.DOUBLE)));

    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.match_phrase_prefix(
                DSL.namedArgument("field", "field_value1"),
                DSL.namedArgument("query", "search query"),
                DSL.namedArgument("slop", "3"),
                DSL.namedArgument("boost", "3.0"))),
        unresolvedPlan);

    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunctions.OpenSearchFunction relevanceQuery =
        (OpenSearchFunctions.OpenSearchFunction) ((LogicalFilter) logicalPlan).getCondition();
    assertEquals(true, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_score_function_with_unsupported_boost_SemanticCheckException() {
    UnresolvedPlan unresolvedPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            new ScoreFunction(
                AstDSL.function(
                    "match_phrase_prefix",
                    AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                    AstDSL.unresolvedArg("query", stringLiteral("search query")),
                    AstDSL.unresolvedArg("boost", stringLiteral("3"))),
                AstDSL.stringLiteral("3.0")));
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> analyze(unresolvedPlan));
    assertEquals("Expected boost type 'DOUBLE' but got 'STRING'", exception.getMessage());
  }

  @Test
  public void head_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.limit(LogicalPlanDSL.relation("schema", table), 10, 0),
        AstDSL.head(AstDSL.relation("schema"), 10, 0));
  }

  @Test
  public void analyze_filter_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        filter(relation("schema"), compare("=", field("integer_value"), intLiteral(1))));
  }

  @Test
  public void analyze_filter_aggregation_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("AVG(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER))),
                    DSL.named("MIN(integer_value)", DSL.min(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING)))),
            DSL.greater( // Expect to be replaced with reference by expression optimizer
                DSL.ref("MIN(integer_value)", INTEGER), DSL.literal(integerValue(10)))),
        AstDSL.filter(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value"))),
                    alias("MIN(integer_value)", aggregate("MIN", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList.of(alias("string_value", qualifiedName("string_value"))),
                emptyList()),
            compare(">", aggregate("MIN", qualifiedName("integer_value")), intLiteral(10))));
  }

  @Test
  public void rename_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.rename(
            LogicalPlanDSL.relation("schema", table),
            ImmutableMap.of(DSL.ref("integer_value", INTEGER), DSL.ref("ivalue", INTEGER))),
        AstDSL.rename(
            AstDSL.relation("schema"),
            AstDSL.map(AstDSL.field("integer_value"), AstDSL.field("ivalue"))));
  }

  @Test
  public void stats_source() {
    assertAnalyzeEqual(
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("schema", table),
            ImmutableList.of(
                DSL.named("avg(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
            ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING)))),
        AstDSL.agg(
            AstDSL.relation("schema"),
            AstDSL.exprList(
                AstDSL.alias(
                    "avg(integer_value)", AstDSL.aggregate("avg", field("integer_value")))),
            null,
            ImmutableList.of(AstDSL.alias("string_value", field("string_value"))),
            AstDSL.defaultStatsArgs()));
  }

  @Test
  public void rare_source() {
    assertAnalyzeEqual(
        LogicalPlanDSL.rareTopN(
            LogicalPlanDSL.relation("schema", table),
            CommandType.RARE,
            10,
            ImmutableList.of(DSL.ref("string_value", STRING)),
            DSL.ref("integer_value", INTEGER)),
        AstDSL.rareTopN(
            AstDSL.relation("schema"),
            CommandType.RARE,
            ImmutableList.of(argument("noOfResults", intLiteral(10))),
            ImmutableList.of(field("string_value")),
            field("integer_value")));
  }

  @Test
  public void top_source() {
    assertAnalyzeEqual(
        LogicalPlanDSL.rareTopN(
            LogicalPlanDSL.relation("schema", table),
            CommandType.TOP,
            5,
            ImmutableList.of(DSL.ref("string_value", STRING)),
            DSL.ref("integer_value", INTEGER)),
        AstDSL.rareTopN(
            AstDSL.relation("schema"),
            CommandType.TOP,
            ImmutableList.of(argument("noOfResults", intLiteral(5))),
            ImmutableList.of(field("string_value")),
            field("integer_value")));
  }

  @Test
  public void rename_to_invalid_expression() {
    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () ->
                analyze(
                    AstDSL.rename(
                        AstDSL.agg(
                            AstDSL.relation("schema"),
                            AstDSL.exprList(
                                AstDSL.alias(
                                    "avg(integer_value)",
                                    AstDSL.aggregate("avg", field("integer_value")))),
                            Collections.emptyList(),
                            ImmutableList.of(),
                            AstDSL.defaultStatsArgs()),
                        AstDSL.map(
                            AstDSL.aggregate("avg", field("integer_value")),
                            AstDSL.aggregate("avg", field("integer_value"))))));
    assertEquals(
        "the target expected to be field, but is avg(Field(field=integer_value, fieldArgs=[]))",
        exception.getMessage());
  }

  @Test
  public void project_source() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.relation("schema", table),
            DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
            DSL.named("double_value", DSL.ref("double_value", DOUBLE))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.field("integer_value"), // Field not wrapped by Alias
            AstDSL.alias("double_value", AstDSL.field("double_value"))));
  }

  @Test
  public void project_nested_field_arg() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)", DSL.nested(DSL.ref("message.info", STRING)), null));

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table), nestedArgs, projectList),
            DSL.named("nested(message.info)", DSL.nested(DSL.ref("message.info", STRING)))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias(
                "nested(message.info)",
                function("nested", qualifiedName("message", "info")),
                null)));

    assertTrue(isNestedFunction(DSL.nested(DSL.ref("message.info", STRING))));
    assertFalse(isNestedFunction(DSL.literal("fieldA")));
    assertFalse(isNestedFunction(DSL.match(DSL.namedArgument("field", literal("message")))));
  }

  @Test
  public void sort_with_nested_all_tuple_fields_throws_exception() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            analyze(
                AstDSL.project(
                    AstDSL.sort(AstDSL.relation("schema"), field(nestedAllTupleFields("message"))),
                    AstDSL.alias("nested(message.*)", nestedAllTupleFields("message")))));
  }

  @Test
  public void filter_with_nested_all_tuple_fields_throws_exception() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            analyze(
                AstDSL.project(
                    AstDSL.filter(
                        AstDSL.relation("schema"),
                        AstDSL.function(
                            "=", nestedAllTupleFields("message"), AstDSL.intLiteral(1))),
                    AstDSL.alias("nested(message.*)", nestedAllTupleFields("message")))));
  }

  @Test
  public void project_nested_field_star_arg() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)", DSL.nested(DSL.ref("message.info", STRING))));

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table), nestedArgs, projectList),
            DSL.named("nested(message.info)", DSL.nested(DSL.ref("message.info", STRING)))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.*)", nestedAllTupleFields("message"))));
  }

  @Test
  public void project_nested_field_star_arg_with_another_nested_function() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)),
            Map.of(
                "field", new ReferenceExpression("comment.data", STRING),
                "path", new ReferenceExpression("comment", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)", DSL.nested(DSL.ref("message.info", STRING))),
            new NamedExpression(
                "nested(comment.data)", DSL.nested(DSL.ref("comment.data", STRING))));

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table), nestedArgs, projectList),
            DSL.named("nested(message.info)", DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("nested(comment.data)", DSL.nested(DSL.ref("comment.data", STRING)))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.*)", nestedAllTupleFields("message")),
            AstDSL.alias("nested(comment.*)", nestedAllTupleFields("comment"))));
  }

  @Test
  public void project_nested_field_star_arg_with_another_field() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)", DSL.nested(DSL.ref("message.info", STRING))),
            new NamedExpression("comment.data", DSL.ref("comment.data", STRING)));

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table), nestedArgs, projectList),
            DSL.named("nested(message.info)", DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("comment.data", DSL.ref("comment.data", STRING))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.*)", nestedAllTupleFields("message")),
            AstDSL.alias("comment.data", field("comment.data"))));
  }

  @Test
  public void project_nested_field_star_arg_with_highlight() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)", DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("highlight(fieldA)", new HighlightExpression(DSL.literal("fieldA"))));

    Map<String, Literal> highlightArgs = new HashMap<>();

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.highlight(
                    LogicalPlanDSL.relation("schema", table), DSL.literal("fieldA"), highlightArgs),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info)", DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("highlight(fieldA)", new HighlightExpression(DSL.literal("fieldA")))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.*)", nestedAllTupleFields("message")),
            AstDSL.alias(
                "highlight(fieldA)",
                new HighlightFunction(AstDSL.stringLiteral("fieldA"), highlightArgs))));
  }

  @Test
  public void project_nested_field_and_path_args() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING), DSL.ref("message", STRING)),
                null));

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table), nestedArgs, projectList),
            DSL.named(
                "nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING), DSL.ref("message", STRING)))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias(
                "nested(message.info)",
                function("nested", qualifiedName("message", "info"), qualifiedName("message")),
                null)));
  }

  @Test
  public void project_nested_deep_field_arg() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info.id", STRING),
                "path", new ReferenceExpression("message.info", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info.id)", DSL.nested(DSL.ref("message.info.id", STRING)), null));

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table), nestedArgs, projectList),
            DSL.named("nested(message.info.id)", DSL.nested(DSL.ref("message.info.id", STRING)))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias(
                "nested(message.info.id)",
                function("nested", qualifiedName("message", "info", "id")),
                null)));
  }

  @Test
  public void project_multiple_nested() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)),
            Map.of(
                "field", new ReferenceExpression("comment.data", STRING),
                "path", new ReferenceExpression("comment", STRING)));

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)", DSL.nested(DSL.ref("message.info", STRING)), null),
            new NamedExpression(
                "nested(comment.data)", DSL.nested(DSL.ref("comment.data", STRING)), null));

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table), nestedArgs, projectList),
            DSL.named("nested(message.info)", DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("nested(comment.data)", DSL.nested(DSL.ref("comment.data", STRING)))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias(
                "nested(message.info)", function("nested", qualifiedName("message", "info")), null),
            AstDSL.alias(
                "nested(comment.data)",
                function("nested", qualifiedName("comment", "data")),
                null)));
  }

  @Test
  public void project_nested_invalid_field_throws_exception() {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                analyze(
                    AstDSL.projectWithArg(
                        AstDSL.relation("schema"),
                        AstDSL.defaultFieldsArgs(),
                        AstDSL.alias(
                            "message", function("nested", qualifiedName("message")), null))));
    assertEquals(exception.getMessage(), "Illegal nested field name: message");
  }

  @Test
  public void project_nested_invalid_arg_type_throws_exception() {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                analyze(
                    AstDSL.projectWithArg(
                        AstDSL.relation("schema"),
                        AstDSL.defaultFieldsArgs(),
                        AstDSL.alias(
                            "message", function("nested", stringLiteral("message")), null))));
    assertEquals(exception.getMessage(), "Illegal nested field name: message");
  }

  @Test
  public void project_nested_no_args_throws_exception() {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                analyze(
                    AstDSL.projectWithArg(
                        AstDSL.relation("schema"),
                        AstDSL.defaultFieldsArgs(),
                        AstDSL.alias("message", function("nested"), null))));
    assertEquals(
        exception.getMessage(),
        "on nested object only allowed 2 parameters (field,path) or 1 parameter (field)");
  }

  @Test
  public void project_nested_too_many_args_throws_exception() {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                analyze(
                    AstDSL.projectWithArg(
                        AstDSL.relation("schema"),
                        AstDSL.defaultFieldsArgs(),
                        AstDSL.alias(
                            "message",
                            function(
                                "nested",
                                stringLiteral("message.info"),
                                stringLiteral("message"),
                                stringLiteral("message")),
                            null))));
    assertEquals(
        exception.getMessage(),
        "on nested object only allowed 2 parameters (field,path) or 1 parameter (field)");
  }

  @Test
  public void project_highlight() {
    Map<String, Literal> args = new HashMap<>();
    args.put("pre_tags", new Literal("<mark>", DataType.STRING));
    args.put("post_tags", new Literal("</mark>", DataType.STRING));

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.highlight(
                LogicalPlanDSL.relation("schema", table), DSL.literal("fieldA"), args),
            DSL.named(
                "highlight(fieldA, pre_tags='<mark>', post_tags='</mark>')",
                new HighlightExpression(DSL.literal("fieldA")))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias(
                "highlight(fieldA, pre_tags='<mark>', post_tags='</mark>')",
                new HighlightFunction(AstDSL.stringLiteral("fieldA"), args))));
  }

  @Test
  public void project_highlight_wildcard() {
    Map<String, Literal> args = new HashMap<>();
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.highlight(
                LogicalPlanDSL.relation("schema", table), DSL.literal("*"), args),
            DSL.named("highlight(*)", new HighlightExpression(DSL.literal("*")))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("highlight(*)", new HighlightFunction(AstDSL.stringLiteral("*"), args))));
  }

  @Test
  public void remove_source() {
    assertAnalyzeEqual(
        LogicalPlanDSL.remove(
            LogicalPlanDSL.relation("schema", table),
            DSL.ref("integer_value", INTEGER),
            DSL.ref("double_value", DOUBLE)),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            Collections.singletonList(argument("exclude", booleanLiteral(true))),
            AstDSL.field("integer_value"),
            AstDSL.field("double_value")));
  }

  @Disabled(
      "the project/remove command should shrink the type env. Should be enabled once "
          + "https://github.com/opensearch-project/sql/issues/917 is resolved")
  @Test
  public void project_source_change_type_env() {
    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () ->
                analyze(
                    AstDSL.projectWithArg(
                        AstDSL.projectWithArg(
                            AstDSL.relation("schema"),
                            AstDSL.defaultFieldsArgs(),
                            AstDSL.field("integer_value"),
                            AstDSL.field("double_value")),
                        AstDSL.defaultFieldsArgs(),
                        AstDSL.field("float_value"))));
  }

  @Test
  public void project_values() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.values(ImmutableList.of(DSL.literal(123))),
            DSL.named("123", DSL.literal(123)),
            DSL.named("hello", DSL.literal("hello")),
            DSL.named("false", DSL.literal(false))),
        AstDSL.project(
            AstDSL.values(ImmutableList.of(AstDSL.intLiteral(123))),
            AstDSL.alias("123", AstDSL.intLiteral(123)),
            AstDSL.alias("hello", AstDSL.stringLiteral("hello")),
            AstDSL.alias("false", AstDSL.booleanLiteral(false))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sort_with_aggregator() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.sort(
                LogicalPlanDSL.aggregation(
                    LogicalPlanDSL.relation("test", table),
                    ImmutableList.of(
                        DSL.named(
                            "avg(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
                    ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING)))),
                // Aggregator in Sort AST node is replaced with reference by expression optimizer
                Pair.of(SortOption.DEFAULT_ASC, DSL.ref("avg(integer_value)", DOUBLE))),
            DSL.named("string_value", DSL.ref("string_value", STRING))),
        AstDSL.project(
            AstDSL.sort(
                AstDSL.agg(
                    AstDSL.relation("test"),
                    ImmutableList.of(
                        AstDSL.alias(
                            "avg(integer_value)", function("avg", qualifiedName("integer_value")))),
                    emptyList(),
                    ImmutableList.of(AstDSL.alias("string_value", qualifiedName("string_value"))),
                    emptyList()),
                field(
                    function("avg", qualifiedName("integer_value")),
                    argument("asc", booleanLiteral(true)))),
            AstDSL.alias("string_value", qualifiedName("string_value"))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sort_with_options() {
    ImmutableMap<Argument[], SortOption> argOptions =
        ImmutableMap.<Argument[], SortOption>builder()
            .put(
                new Argument[] {argument("asc", booleanLiteral(true))},
                new SortOption(SortOrder.ASC, NullOrder.NULL_FIRST))
            .put(
                new Argument[] {argument("asc", booleanLiteral(false))},
                new SortOption(SortOrder.DESC, NullOrder.NULL_LAST))
            .put(
                new Argument[] {
                  argument("asc", booleanLiteral(true)), argument("nullFirst", booleanLiteral(true))
                },
                new SortOption(SortOrder.ASC, NullOrder.NULL_FIRST))
            .put(
                new Argument[] {
                  argument("asc", booleanLiteral(true)),
                  argument("nullFirst", booleanLiteral(false))
                },
                new SortOption(SortOrder.ASC, NullOrder.NULL_LAST))
            .put(
                new Argument[] {
                  argument("asc", booleanLiteral(false)),
                  argument("nullFirst", booleanLiteral(true))
                },
                new SortOption(SortOrder.DESC, NullOrder.NULL_FIRST))
            .put(
                new Argument[] {
                  argument("asc", booleanLiteral(false)),
                  argument("nullFirst", booleanLiteral(false))
                },
                new SortOption(SortOrder.DESC, NullOrder.NULL_LAST))
            .build();

    argOptions.forEach(
        (args, expectOption) ->
            assertAnalyzeEqual(
                LogicalPlanDSL.project(
                    LogicalPlanDSL.sort(
                        LogicalPlanDSL.relation("test", table),
                        Pair.of(expectOption, DSL.ref("integer_value", INTEGER))),
                    DSL.named("string_value", DSL.ref("string_value", STRING))),
                AstDSL.project(
                    AstDSL.sort(
                        AstDSL.relation("test"), field(qualifiedName("integer_value"), args)),
                    AstDSL.alias("string_value", qualifiedName("string_value")))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void window_function() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.window(
                LogicalPlanDSL.sort(
                    LogicalPlanDSL.relation("test", table),
                    ImmutablePair.of(DEFAULT_ASC, DSL.ref("string_value", STRING)),
                    ImmutablePair.of(DEFAULT_ASC, DSL.ref("integer_value", INTEGER))),
                DSL.named("window_function", DSL.rowNumber()),
                new WindowDefinition(
                    ImmutableList.of(DSL.ref("string_value", STRING)),
                    ImmutableList.of(
                        ImmutablePair.of(DEFAULT_ASC, DSL.ref("integer_value", INTEGER))))),
            DSL.named("string_value", DSL.ref("string_value", STRING)),
            // Alias name "window_function" is used as internal symbol name to connect
            // project item and window operator output
            DSL.named("window_function", DSL.ref("window_function", INTEGER))),
        AstDSL.project(
            AstDSL.relation("test"),
            AstDSL.alias("string_value", AstDSL.qualifiedName("string_value")),
            AstDSL.alias(
                "window_function",
                AstDSL.window(
                    AstDSL.function("row_number"),
                    Collections.singletonList(AstDSL.qualifiedName("string_value")),
                    Collections.singletonList(
                        ImmutablePair.of(DEFAULT_ASC, AstDSL.qualifiedName("integer_value")))))));
  }

  /** SELECT name FROM ( SELECT name, age FROM test ) AS schema. */
  @Test
  public void from_subquery() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.project(
                LogicalPlanDSL.relation("schema", table),
                DSL.named("string_value", DSL.ref("string_value", STRING)),
                DSL.named("integer_value", DSL.ref("integer_value", INTEGER))),
            DSL.named("string_value", DSL.ref("string_value", STRING))),
        AstDSL.project(
            AstDSL.relationSubquery(
                AstDSL.project(
                    AstDSL.relation("schema"),
                    AstDSL.alias("string_value", AstDSL.qualifiedName("string_value")),
                    AstDSL.alias("integer_value", AstDSL.qualifiedName("integer_value"))),
                "schema"),
            AstDSL.alias("string_value", AstDSL.qualifiedName("string_value"))));
  }

  /** SELECT * FROM ( SELECT name FROM test ) AS schema. */
  @Test
  public void select_all_from_subquery() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.project(
                LogicalPlanDSL.relation("schema", table),
                DSL.named("string_value", DSL.ref("string_value", STRING))),
            DSL.named("string_value", DSL.ref("string_value", STRING))),
        AstDSL.project(
            AstDSL.relationSubquery(
                AstDSL.project(
                    AstDSL.relation("schema"),
                    AstDSL.alias("string_value", AstDSL.qualifiedName("string_value"))),
                "schema"),
            AstDSL.allFields()));
  }

  /**
   * Ensure Nested function falls back to legacy engine when used in GROUP BY clause. TODO Remove
   * this test when support is added.
   */
  @Test
  public void nested_group_by_clause_throws_syntax_exception() {
    SyntaxCheckException exception =
        assertThrows(
            SyntaxCheckException.class,
            () ->
                analyze(
                    AstDSL.project(
                        AstDSL.agg(
                            AstDSL.relation("schema"),
                            emptyList(),
                            emptyList(),
                            ImmutableList.of(
                                alias(
                                    "nested(message.info)",
                                    function("nested", qualifiedName("message", "info")))),
                            emptyList()))));
    assertEquals(
        "Falling back to legacy engine. Nested function is not supported in WHERE,"
            + " GROUP BY, and HAVING clauses.",
        exception.getMessage());
  }

  /** SELECT name, AVG(age) FROM test GROUP BY name. */
  @Test
  public void sql_group_by_field() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("AVG(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING)))),
            DSL.named("string_value", DSL.ref("string_value", STRING)),
            DSL.named("AVG(integer_value)", DSL.ref("AVG(integer_value)", DOUBLE))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList.of(alias("string_value", qualifiedName("string_value"))),
                emptyList()),
            AstDSL.alias("string_value", qualifiedName("string_value")),
            AstDSL.alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value")))));
  }

  /** SELECT abs(name), AVG(age) FROM test GROUP BY abs(name). */
  @Test
  public void sql_group_by_function() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("AVG(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(
                    DSL.named("abs(long_value)", DSL.abs(DSL.ref("long_value", LONG))))),
            DSL.named("abs(long_value)", DSL.ref("abs(long_value)", LONG)),
            DSL.named("AVG(integer_value)", DSL.ref("AVG(integer_value)", DOUBLE))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList.of(
                    alias("abs(long_value)", function("abs", qualifiedName("long_value")))),
                emptyList()),
            AstDSL.alias("abs(long_value)", function("abs", qualifiedName("long_value"))),
            AstDSL.alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value")))));
  }

  /** SELECT abs(name), AVG(age) FROM test GROUP BY ABS(name). */
  @Test
  public void sql_group_by_function_in_uppercase() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("AVG(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(
                    DSL.named("ABS(long_value)", DSL.abs(DSL.ref("long_value", LONG))))),
            DSL.named("abs(long_value)", DSL.ref("ABS(long_value)", LONG)),
            DSL.named("AVG(integer_value)", DSL.ref("AVG(integer_value)", DOUBLE))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList.of(
                    alias("ABS(long_value)", function("ABS", qualifiedName("long_value")))),
                emptyList()),
            AstDSL.alias("abs(long_value)", function("abs", qualifiedName("long_value"))),
            AstDSL.alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value")))));
  }

  /** SELECT abs(name), abs(avg(age) FROM test GROUP BY abs(name). */
  @Test
  public void sql_expression_over_one_aggregation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("avg(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(
                    DSL.named("abs(long_value)", DSL.abs(DSL.ref("long_value", LONG))))),
            DSL.named("abs(long_value)", DSL.ref("abs(long_value)", LONG)),
            DSL.named("abs(avg(integer_value)", DSL.abs(DSL.ref("avg(integer_value)", DOUBLE)))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("avg(integer_value)", aggregate("avg", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList.of(
                    alias("abs(long_value)", function("abs", qualifiedName("long_value")))),
                emptyList()),
            AstDSL.alias("abs(long_value)", function("abs", qualifiedName("long_value"))),
            AstDSL.alias(
                "abs(avg(integer_value)",
                function("abs", aggregate("avg", qualifiedName("integer_value"))))));
  }

  /** SELECT abs(name), sum(age)-avg(age) FROM test GROUP BY abs(name). */
  @Test
  public void sql_expression_over_two_aggregation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("sum(integer_value)", DSL.sum(DSL.ref("integer_value", INTEGER))),
                    DSL.named("avg(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(
                    DSL.named("abs(long_value)", DSL.abs(DSL.ref("long_value", LONG))))),
            DSL.named("abs(long_value)", DSL.ref("abs(long_value)", LONG)),
            DSL.named(
                "sum(integer_value)-avg(integer_value)",
                DSL.subtract(
                    DSL.ref("sum(integer_value)", INTEGER),
                    DSL.ref("avg(integer_value)", DOUBLE)))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("sum(integer_value)", aggregate("sum", qualifiedName("integer_value"))),
                    alias("avg(integer_value)", aggregate("avg", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList.of(
                    alias("abs(long_value)", function("abs", qualifiedName("long_value")))),
                emptyList()),
            AstDSL.alias("abs(long_value)", function("abs", qualifiedName("long_value"))),
            AstDSL.alias(
                "sum(integer_value)-avg(integer_value)",
                function(
                    "-",
                    aggregate("sum", qualifiedName("integer_value")),
                    aggregate("avg", qualifiedName("integer_value"))))));
  }

  @Test
  public void limit_offset() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.limit(LogicalPlanDSL.relation("schema", table), 1, 1),
            DSL.named("integer_value", DSL.ref("integer_value", INTEGER))),
        AstDSL.project(
            AstDSL.limit(AstDSL.relation("schema"), 1, 1),
            AstDSL.alias("integer_value", qualifiedName("integer_value"))));
  }

  /**
   * SELECT COUNT(NAME) FILTER(WHERE age > 1) FROM test. This test is to verify that the aggregator
   * properties are taken when wrapping it to {@link
   * org.opensearch.sql.expression.aggregation.NamedAggregator}
   */
  @Test
  public void named_aggregator_with_condition() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named(
                        "count(string_value) filter(where integer_value > 1)",
                        DSL.count(DSL.ref("string_value", STRING))
                            .condition(
                                DSL.greater(DSL.ref("integer_value", INTEGER), DSL.literal(1))))),
                emptyList()),
            DSL.named(
                "count(string_value) filter(where integer_value > 1)",
                DSL.ref("count(string_value) filter(where integer_value > 1)", INTEGER))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias(
                        "count(string_value) filter(where integer_value > 1)",
                        filteredAggregate(
                            "count",
                            qualifiedName("string_value"),
                            function(">", qualifiedName("integer_value"), intLiteral(1))))),
                emptyList(),
                emptyList(),
                emptyList()),
            AstDSL.alias(
                "count(string_value) filter(where integer_value > 1)",
                filteredAggregate(
                    "count",
                    qualifiedName("string_value"),
                    function(">", qualifiedName("integer_value"), intLiteral(1))))));
  }

  /** stats avg(integer_value) by string_value span(long_value, 10). */
  @Test
  public void ppl_stats_by_fieldAndSpan() {
    assertAnalyzeEqual(
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("schema", table),
            ImmutableList.of(
                DSL.named("AVG(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
            ImmutableList.of(
                DSL.named("span", DSL.span(DSL.ref("long_value", LONG), DSL.literal(10), "")),
                DSL.named("string_value", DSL.ref("string_value", STRING)))),
        AstDSL.agg(
            AstDSL.relation("schema"),
            ImmutableList.of(
                alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value")))),
            emptyList(),
            ImmutableList.of(alias("string_value", qualifiedName("string_value"))),
            alias("span", span(field("long_value"), intLiteral(10), SpanUnit.NONE)),
            emptyList()));
  }

  @Test
  public void parse_relation_with_grok_expression() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.relation("schema", table),
            ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING))),
            ImmutableList.of(
                DSL.named(
                    "grok_field",
                    DSL.grok(
                        DSL.ref("string_value", STRING),
                        DSL.literal("%{IPV4:grok_field}"),
                        DSL.literal("grok_field"))))),
        AstDSL.project(
            AstDSL.parse(
                AstDSL.relation("schema"),
                ParseMethod.GROK,
                AstDSL.field("string_value"),
                AstDSL.stringLiteral("%{IPV4:grok_field}"),
                ImmutableMap.of()),
            AstDSL.alias("string_value", qualifiedName("string_value"))));
  }

  @Test
  public void parse_relation_with_regex_expression() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.relation("schema", table),
            ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING))),
            ImmutableList.of(
                DSL.named(
                    "group",
                    DSL.regex(
                        DSL.ref("string_value", STRING),
                        DSL.literal("(?<group>.*)"),
                        DSL.literal("group"))))),
        AstDSL.project(
            AstDSL.parse(
                AstDSL.relation("schema"),
                ParseMethod.REGEX,
                AstDSL.field("string_value"),
                AstDSL.stringLiteral("(?<group>.*)"),
                ImmutableMap.of()),
            AstDSL.alias("string_value", qualifiedName("string_value"))));
  }

  @Test
  public void parse_relation_with_patterns_expression() {
    Map<String, Literal> arguments =
        ImmutableMap.<String, Literal>builder()
            .put("new_field", AstDSL.stringLiteral("custom_field"))
            .put("pattern", AstDSL.stringLiteral("custom_pattern"))
            .build();

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.relation("schema", table),
            ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING))),
            ImmutableList.of(
                DSL.named(
                    "custom_field",
                    DSL.patterns(
                        DSL.ref("string_value", STRING),
                        DSL.literal("custom_pattern"),
                        DSL.literal("custom_field"))))),
        AstDSL.project(
            AstDSL.parse(
                AstDSL.relation("schema"),
                ParseMethod.PATTERNS,
                AstDSL.field("string_value"),
                AstDSL.stringLiteral("custom_pattern"),
                arguments),
            AstDSL.alias("string_value", qualifiedName("string_value"))));
  }

  @Test
  public void parse_relation_with_patterns_expression_no_args() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.relation("schema", table),
            ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING))),
            ImmutableList.of(
                DSL.named(
                    "patterns_field",
                    DSL.patterns(
                        DSL.ref("string_value", STRING),
                        DSL.literal(""),
                        DSL.literal("patterns_field"))))),
        AstDSL.project(
            AstDSL.parse(
                AstDSL.relation("schema"),
                ParseMethod.PATTERNS,
                AstDSL.field("string_value"),
                AstDSL.stringLiteral(""),
                ImmutableMap.of()),
            AstDSL.alias("string_value", qualifiedName("string_value"))));
  }

  @Test
  public void kmeanns_relation() {
    Map<String, Literal> argumentMap =
        new HashMap<String, Literal>() {
          {
            put("centroids", new Literal(3, DataType.INTEGER));
            put("iterations", new Literal(2, DataType.INTEGER));
            put("distance_type", new Literal("COSINE", DataType.STRING));
          }
        };
    assertAnalyzeEqual(
        new LogicalMLCommons(LogicalPlanDSL.relation("schema", table), "kmeans", argumentMap),
        new Kmeans(AstDSL.relation("schema"), argumentMap));
  }

  @Test
  public void fillnull_same_value() {
    assertAnalyzeEqual(
        LogicalPlanDSL.eval(
            LogicalPlanDSL.relation("schema", table),
            ImmutablePair.of(
                DSL.ref("integer_value", INTEGER),
                DSL.ifnull(DSL.ref("integer_value", INTEGER), DSL.literal(0))),
            ImmutablePair.of(
                DSL.ref("int_null_value", INTEGER),
                DSL.ifnull(DSL.ref("int_null_value", INTEGER), DSL.literal(0)))),
        new FillNull(
            AstDSL.relation("schema"),
            FillNull.ContainNullableFieldFill.ofSameValue(
                AstDSL.intLiteral(0),
                ImmutableList.<Field>builder()
                    .add(AstDSL.field("integer_value"))
                    .add(AstDSL.field("int_null_value"))
                    .build())));
  }

  @Test
  public void fillnull_various_values() {
    assertAnalyzeEqual(
        LogicalPlanDSL.eval(
            LogicalPlanDSL.relation("schema", table),
            ImmutablePair.of(
                DSL.ref("integer_value", INTEGER),
                DSL.ifnull(DSL.ref("integer_value", INTEGER), DSL.literal(0))),
            ImmutablePair.of(
                DSL.ref("int_null_value", INTEGER),
                DSL.ifnull(DSL.ref("int_null_value", INTEGER), DSL.literal(1)))),
        new FillNull(
            AstDSL.relation("schema"),
            FillNull.ContainNullableFieldFill.ofVariousValue(
                ImmutableList.of(
                    new FillNull.NullableFieldFill(
                        AstDSL.field("integer_value"), AstDSL.intLiteral(0)),
                    new FillNull.NullableFieldFill(
                        AstDSL.field("int_null_value"), AstDSL.intLiteral(1))))));
  }

  @Test
  public void trendline() {
    assertAnalyzeEqual(
        LogicalPlanDSL.trendline(
            LogicalPlanDSL.relation("schema", table),
            Pair.of(computation(5, field("float_value"), "test_field_alias", "sma"), DOUBLE),
            Pair.of(computation(1, field("double_value"), "test_field_alias_2", "sma"), DOUBLE)),
        AstDSL.trendline(
            AstDSL.relation("schema"),
            computation(5, field("float_value"), "test_field_alias", "sma"),
            computation(1, field("double_value"), "test_field_alias_2", "sma")));
  }

  @Test
  public void ad_batchRCF_relation() {
    Map<String, Literal> argumentMap =
        new HashMap<String, Literal>() {
          {
            put("shingle_size", new Literal(8, DataType.INTEGER));
          }
        };
    assertAnalyzeEqual(
        new LogicalAD(LogicalPlanDSL.relation("schema", table), argumentMap),
        new AD(AstDSL.relation("schema"), argumentMap));
  }

  @Test
  public void ad_fitRCF_relation() {
    Map<String, Literal> argumentMap =
        new HashMap<String, Literal>() {
          {
            put("shingle_size", new Literal(8, DataType.INTEGER));
            put("time_decay", new Literal(0.0001, DataType.DOUBLE));
            put("time_field", new Literal("timestamp", DataType.STRING));
          }
        };
    assertAnalyzeEqual(
        new LogicalAD(LogicalPlanDSL.relation("schema", table), argumentMap),
        new AD(AstDSL.relation("schema"), argumentMap));
  }

  @Test
  public void ad_fitRCF_relation_with_time_field() {
    Map<String, Literal> argumentMap =
        new HashMap<String, Literal>() {
          {
            put("shingle_size", new Literal(8, DataType.INTEGER));
            put("time_decay", new Literal(0.0001, DataType.DOUBLE));
            put("time_field", new Literal("ts", DataType.STRING));
          }
        };

    LogicalPlan actual =
        analyze(AstDSL.project(new AD(AstDSL.relation("schema"), argumentMap), AstDSL.allFields()));
    assertTrue(((LogicalProject) actual).getProjectList().size() >= 3);
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named("score", DSL.ref("score", DOUBLE))));
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named("anomaly_grade", DSL.ref("anomaly_grade", DOUBLE))));
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named("ts", DSL.ref("ts", TIMESTAMP))));
  }

  @Test
  public void ad_fitRCF_relation_without_time_field() {
    Map<String, Literal> argumentMap =
        new HashMap<>() {
          {
            put("shingle_size", new Literal(8, DataType.INTEGER));
            put("time_decay", new Literal(0.0001, DataType.DOUBLE));
          }
        };

    LogicalPlan actual =
        analyze(AstDSL.project(new AD(AstDSL.relation("schema"), argumentMap), AstDSL.allFields()));
    assertTrue(((LogicalProject) actual).getProjectList().size() >= 2);
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named("score", DSL.ref("score", DOUBLE))));
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named("anomalous", DSL.ref("anomalous", BOOLEAN))));
  }

  @Test
  public void table_function() {
    assertAnalyzeEqual(
        new LogicalRelation("query_range", table),
        AstDSL.tableFunction(
            List.of("prometheus", "query_range"),
            unresolvedArg("query", stringLiteral("http_latency")),
            unresolvedArg("starttime", intLiteral(12345)),
            unresolvedArg("endtime", intLiteral(12345)),
            unresolvedArg("step", intLiteral(14))));
  }

  @Test
  public void table_function_with_no_datasource() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () ->
                analyze(
                    AstDSL.tableFunction(
                        List.of("query_range"),
                        unresolvedArg("query", stringLiteral("http_latency")),
                        unresolvedArg("", intLiteral(12345)),
                        unresolvedArg("", intLiteral(12345)),
                        unresolvedArg(null, intLiteral(14)))));
    assertEquals("unsupported function name: query_range", exception.getMessage());
  }

  @Test
  public void table_function_with_wrong_datasource() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () ->
                analyze(
                    AstDSL.tableFunction(
                        Arrays.asList("prome", "query_range"),
                        unresolvedArg("query", stringLiteral("http_latency")),
                        unresolvedArg("", intLiteral(12345)),
                        unresolvedArg("", intLiteral(12345)),
                        unresolvedArg(null, intLiteral(14)))));
    assertEquals("unsupported function name: prome.query_range", exception.getMessage());
  }

  @Test
  public void table_function_with_wrong_table_function() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () ->
                analyze(
                    AstDSL.tableFunction(
                        Arrays.asList("prometheus", "queryrange"),
                        unresolvedArg("query", stringLiteral("http_latency")),
                        unresolvedArg("", intLiteral(12345)),
                        unresolvedArg("", intLiteral(12345)),
                        unresolvedArg(null, intLiteral(14)))));
    assertEquals("unsupported function name: queryrange", exception.getMessage());
  }

  @Test
  public void show_datasources() {
    assertAnalyzeEqual(
        new LogicalRelation(DATASOURCES_TABLE_NAME, new DataSourceTable(dataSourceService)),
        AstDSL.relation(qualifiedName(DATASOURCES_TABLE_NAME)));
  }

  @Test
  public void ml_relation_unsupported_action() {
    Map<String, Literal> argumentMap =
        new HashMap<>() {
          {
            put(ACTION, new Literal("unsupported", DataType.STRING));
            put(ALGO, new Literal(KMEANS, DataType.STRING));
          }
        };

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                analyze(
                    AstDSL.project(
                        new ML(AstDSL.relation("schema"), argumentMap), AstDSL.allFields())));
    assertEquals(
        "Action error. Please indicate train, predict or trainandpredict.", exception.getMessage());
  }

  @Test
  public void ml_relation_unsupported_algorithm() {
    Map<String, Literal> argumentMap =
        new HashMap<>() {
          {
            put(ACTION, new Literal(PREDICT, DataType.STRING));
            put(ALGO, new Literal("unsupported", DataType.STRING));
          }
        };

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                analyze(
                    AstDSL.project(
                        new ML(AstDSL.relation("schema"), argumentMap), AstDSL.allFields())));
    assertEquals("Unsupported algorithm: unsupported", exception.getMessage());
  }

  @Test
  public void ml_relation_train_sync() {
    Map<String, Literal> argumentMap =
        new HashMap<>() {
          {
            put(ACTION, new Literal(TRAIN, DataType.STRING));
            put(ALGO, new Literal(KMEANS, DataType.STRING));
          }
        };

    LogicalPlan actual =
        analyze(AstDSL.project(new ML(AstDSL.relation("schema"), argumentMap), AstDSL.allFields()));
    assertTrue(((LogicalProject) actual).getProjectList().size() >= 2);
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(STATUS, DSL.ref(STATUS, STRING))));
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(MODELID, DSL.ref(MODELID, STRING))));
  }

  @Test
  public void ml_relation_train_async() {
    Map<String, Literal> argumentMap =
        new HashMap<>() {
          {
            put(ACTION, new Literal(TRAIN, DataType.STRING));
            put(ALGO, new Literal(KMEANS, DataType.STRING));
            put(ASYNC, new Literal(true, DataType.BOOLEAN));
          }
        };

    LogicalPlan actual =
        analyze(AstDSL.project(new ML(AstDSL.relation("schema"), argumentMap), AstDSL.allFields()));
    assertTrue(((LogicalProject) actual).getProjectList().size() >= 2);
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(STATUS, DSL.ref(STATUS, STRING))));
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(TASKID, DSL.ref(TASKID, STRING))));
  }

  @Test
  public void ml_relation_predict_kmeans() {
    Map<String, Literal> argumentMap =
        new HashMap<>() {
          {
            put(ACTION, new Literal(PREDICT, DataType.STRING));
            put(ALGO, new Literal(KMEANS, DataType.STRING));
          }
        };

    LogicalPlan actual =
        analyze(AstDSL.project(new ML(AstDSL.relation("schema"), argumentMap), AstDSL.allFields()));
    assertTrue(((LogicalProject) actual).getProjectList().size() >= 1);
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(CLUSTERID, DSL.ref(CLUSTERID, INTEGER))));
  }

  @Test
  public void ml_relation_predict_rcf_with_time_field() {
    Map<String, Literal> argumentMap =
        new HashMap<>() {
          {
            put(ACTION, new Literal(PREDICT, DataType.STRING));
            put(ALGO, new Literal(RCF, DataType.STRING));
            put(RCF_TIME_FIELD, new Literal("ts", DataType.STRING));
          }
        };

    LogicalPlan actual =
        analyze(AstDSL.project(new ML(AstDSL.relation("schema"), argumentMap), AstDSL.allFields()));
    assertTrue(((LogicalProject) actual).getProjectList().size() >= 3);
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(RCF_SCORE, DSL.ref(RCF_SCORE, DOUBLE))));
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(RCF_ANOMALY_GRADE, DSL.ref(RCF_ANOMALY_GRADE, DOUBLE))));
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named("ts", DSL.ref("ts", TIMESTAMP))));
  }

  @Test
  public void ml_relation_predict_rcf_without_time_field() {
    Map<String, Literal> argumentMap =
        new HashMap<>() {
          {
            put(ACTION, new Literal(PREDICT, DataType.STRING));
            put(ALGO, new Literal(RCF, DataType.STRING));
          }
        };

    LogicalPlan actual =
        analyze(AstDSL.project(new ML(AstDSL.relation("schema"), argumentMap), AstDSL.allFields()));
    assertTrue(((LogicalProject) actual).getProjectList().size() >= 2);
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(RCF_SCORE, DSL.ref(RCF_SCORE, DOUBLE))));
    assertTrue(
        ((LogicalProject) actual)
            .getProjectList()
            .contains(DSL.named(RCF_ANOMALOUS, DSL.ref(RCF_ANOMALOUS, BOOLEAN))));
  }

  @Test
  public void visit_paginate() {
    LogicalPlan actual = analyze(new Paginate(10, AstDSL.relation("dummy")));
    assertTrue(actual instanceof LogicalPaginate);
    assertEquals(10, ((LogicalPaginate) actual).getPageSize());
  }

  @Test
  void visit_cursor() {
    LogicalPlan actual = analyze((new FetchCursor("test")));
    assertTrue(actual instanceof LogicalFetchCursor);
    assertEquals(
        new LogicalFetchCursor(
            "test", dataSourceService.getDataSource("@opensearch").getStorageEngine()),
        actual);
  }

  @Test
  public void visit_close_cursor() {
    var analyzed = analyze(new CloseCursor().attach(new FetchCursor("pewpew")));
    assertAll(
        () -> assertTrue(analyzed instanceof LogicalCloseCursor),
        () -> assertTrue(analyzed.getChild().get(0) instanceof LogicalFetchCursor),
        () ->
            assertEquals("pewpew", ((LogicalFetchCursor) analyzed.getChild().get(0)).getCursor()));
  }
}

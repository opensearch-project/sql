/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.filteredAggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
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
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.planner.logical.LogicalAD;
import org.opensearch.sql.planner.logical.LogicalMLCommons;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTest.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class AnalyzerTest extends AnalyzerTestBase {

  @Test
  public void filter_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            dsl.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation("schema"),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_catalog() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("http_total_requests", table),
            dsl.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("prometheus", "http_total_requests")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_escaped_catalog() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("prometheus.http_total_requests", table),
            dsl.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("prometheus.http_total_requests")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void filter_relation_with_non_existing_catalog() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("test.http_total_requests", table),
            dsl.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        AstDSL.filter(
            AstDSL.relation(AstDSL.qualifiedName("test", "http_total_requests")),
            AstDSL.equalTo(AstDSL.field("integer_value"), AstDSL.intLiteral(1))));
  }

  @Test
  public void head_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.limit(LogicalPlanDSL.relation("schema", table),
            10, 0),
        AstDSL.head(AstDSL.relation("schema"), 10, 0));
  }

  @Test
  public void analyze_filter_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            dsl.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))),
        filter(relation("schema"), compare("=", field("integer_value"), intLiteral(1))));
  }

  @Test
  public void analyze_filter_aggregation_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("AVG(integer_value)", dsl.avg(DSL.ref("integer_value", INTEGER))),
                    DSL.named("MIN(integer_value)", dsl.min(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING)))),
            dsl.greater(// Expect to be replaced with reference by expression optimizer
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
            compare(">",
                aggregate("MIN", qualifiedName("integer_value")), intLiteral(10))));
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
            ImmutableList
                .of(DSL.named("avg(integer_value)", dsl.avg(DSL.ref("integer_value", INTEGER)))),
            ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING)))),
        AstDSL.agg(
            AstDSL.relation("schema"),
            AstDSL.exprList(
                AstDSL.alias(
                    "avg(integer_value)",
                    AstDSL.aggregate("avg", field("integer_value")))
            ),
            null,
            ImmutableList.of(
                AstDSL.alias("string_value", field("string_value"))),
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
            DSL.ref("integer_value", INTEGER)
        ),
        AstDSL.rareTopN(
            AstDSL.relation("schema"),
            CommandType.RARE,
            ImmutableList.of(argument("noOfResults", intLiteral(10))),
            ImmutableList.of(field("string_value")),
            field("integer_value")
        )
    );
  }

  @Test
  public void top_source() {
    assertAnalyzeEqual(
        LogicalPlanDSL.rareTopN(
            LogicalPlanDSL.relation("schema", table),
            CommandType.TOP,
            5,
            ImmutableList.of(DSL.ref("string_value", STRING)),
            DSL.ref("integer_value", INTEGER)
        ),
        AstDSL.rareTopN(
            AstDSL.relation("schema"),
            CommandType.TOP,
            ImmutableList.of(argument("noOfResults", intLiteral(5))),
            ImmutableList.of(field("string_value")),
            field("integer_value")
        )
    );
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
                                AstDSL.alias("avg(integer_value)", AstDSL.aggregate("avg", field(
                                    "integer_value")))),
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
            DSL.named("double_value", DSL.ref("double_value", DOUBLE))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.field("integer_value"), // Field not wrapped by Alias
            AstDSL.alias("double_value", AstDSL.field("double_value"))));
  }

  @Test
  public void project_highlight() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.highlight(LogicalPlanDSL.relation("schema", table),
                DSL.literal("fieldA")),
            DSL.named("highlight(fieldA)", new HighlightExpression(DSL.literal("fieldA")))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("highlight(fieldA)", new HighlightFunction(AstDSL.stringLiteral("fieldA")))
        )
    );
  }

  @Test
  public void remove_source() {
    assertAnalyzeEqual(
        LogicalPlanDSL.remove(
            LogicalPlanDSL.relation("schema", table),
            DSL.ref("integer_value", INTEGER), DSL.ref(
                "double_value", DOUBLE)),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            Collections.singletonList(argument("exclude", booleanLiteral(true))),
            AstDSL.field("integer_value"),
            AstDSL.field("double_value")));
  }

  @Disabled("the project/remove command should shrink the type env")
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
            DSL.named("false", DSL.literal(false))
        ),
        AstDSL.project(
            AstDSL.values(ImmutableList.of(AstDSL.intLiteral(123))),
            AstDSL.alias("123", AstDSL.intLiteral(123)),
            AstDSL.alias("hello", AstDSL.stringLiteral("hello")),
            AstDSL.alias("false", AstDSL.booleanLiteral(false))
        )
    );
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
                            "avg(integer_value)",
                            dsl.avg(DSL.ref("integer_value", INTEGER)))),
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
                            "avg(integer_value)",
                            function("avg", qualifiedName("integer_value")))),
                    emptyList(),
                    ImmutableList.of(AstDSL.alias("string_value", qualifiedName("string_value"))),
                    emptyList()
                ),
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
            .put(new Argument[] {argument("asc", booleanLiteral(true))},
                new SortOption(SortOrder.ASC, NullOrder.NULL_FIRST))
            .put(new Argument[] {argument("asc", booleanLiteral(false))},
                new SortOption(SortOrder.DESC, NullOrder.NULL_LAST))
            .put(new Argument[] {
                    argument("asc", booleanLiteral(true)),
                    argument("nullFirst", booleanLiteral(true))},
                new SortOption(SortOrder.ASC, NullOrder.NULL_FIRST))
            .put(new Argument[] {
                    argument("asc", booleanLiteral(true)),
                    argument("nullFirst", booleanLiteral(false))},
                new SortOption(SortOrder.ASC, NullOrder.NULL_LAST))
            .put(new Argument[] {
                    argument("asc", booleanLiteral(false)),
                    argument("nullFirst", booleanLiteral(true))},
                new SortOption(SortOrder.DESC, NullOrder.NULL_FIRST))
            .put(new Argument[] {
                    argument("asc", booleanLiteral(false)),
                    argument("nullFirst", booleanLiteral(false))},
                new SortOption(SortOrder.DESC, NullOrder.NULL_LAST))
            .build();

    argOptions.forEach((args, expectOption) ->
        assertAnalyzeEqual(
            LogicalPlanDSL.project(
                LogicalPlanDSL.sort(
                    LogicalPlanDSL.relation("test", table),
                    Pair.of(expectOption, DSL.ref("integer_value", INTEGER))),
                DSL.named("string_value", DSL.ref("string_value", STRING))),
            AstDSL.project(
                AstDSL.sort(
                    AstDSL.relation("test"),
                    field(qualifiedName("integer_value"), args)),
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
                DSL.named("window_function", dsl.rowNumber()),
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
            AstDSL.alias("window_function",
                AstDSL.window(
                    AstDSL.function("row_number"),
                    Collections.singletonList(AstDSL.qualifiedName("string_value")),
                    Collections.singletonList(
                        ImmutablePair.of(DEFAULT_ASC, AstDSL.qualifiedName("integer_value")))))));
  }

  /**
   * SELECT name FROM (
   * SELECT name, age FROM test
   * ) AS schema.
   */
  @Test
  public void from_subquery() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.project(
                LogicalPlanDSL.relation("schema", table),
                DSL.named("string_value", DSL.ref("string_value", STRING)),
                DSL.named("integer_value", DSL.ref("integer_value", INTEGER))
            ),
            DSL.named("string_value", DSL.ref("string_value", STRING))
        ),
        AstDSL.project(
            AstDSL.relationSubquery(
                AstDSL.project(
                    AstDSL.relation("schema"),
                    AstDSL.alias("string_value", AstDSL.qualifiedName("string_value")),
                    AstDSL.alias("integer_value", AstDSL.qualifiedName("integer_value"))
                ),
                "schema"
            ),
            AstDSL.alias("string_value", AstDSL.qualifiedName("string_value"))
        )
    );
  }

  /**
   * SELECT * FROM (
   * SELECT name FROM test
   * ) AS schema.
   */
  @Test
  public void select_all_from_subquery() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.project(
                LogicalPlanDSL.relation("schema", table),
                DSL.named("string_value", DSL.ref("string_value", STRING))),
            DSL.named("string_value", DSL.ref("string_value", STRING))
        ),
        AstDSL.project(
            AstDSL.relationSubquery(
                AstDSL.project(
                    AstDSL.relation("schema"),
                    AstDSL.alias("string_value", AstDSL.qualifiedName("string_value"))
                ),
                "schema"
            ),
            AstDSL.allFields()
        )
    );
  }

  /**
   * SELECT name, AVG(age) FROM test GROUP BY name.
   */
  @Test
  public void sql_group_by_field() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList
                    .of(DSL
                        .named("AVG(integer_value)", dsl.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING)))),
            DSL.named("string_value", DSL.ref("string_value", STRING)),
            DSL.named("AVG(integer_value)", DSL.ref("AVG(integer_value)", DOUBLE))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(alias("AVG(integer_value)",
                    aggregate("AVG", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList.of(alias("string_value", qualifiedName("string_value"))),
                emptyList()),
            AstDSL.alias("string_value", qualifiedName("string_value")),
            AstDSL.alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value"))))
    );
  }

  /**
   * SELECT abs(name), AVG(age) FROM test GROUP BY abs(name).
   */
  @Test
  public void sql_group_by_function() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList
                    .of(DSL
                        .named("AVG(integer_value)", dsl.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("abs(long_value)",
                    dsl.abs(DSL.ref("long_value", LONG))))),
            DSL.named("abs(long_value)", DSL.ref("abs(long_value)", LONG)),
            DSL.named("AVG(integer_value)", DSL.ref("AVG(integer_value)", DOUBLE))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(alias("AVG(integer_value)",
                    aggregate("AVG", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList
                    .of(alias("abs(long_value)", function("abs", qualifiedName("long_value")))),
                emptyList()),
            AstDSL.alias("abs(long_value)", function("abs", qualifiedName("long_value"))),
            AstDSL.alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value"))))
    );
  }

  /**
   * SELECT abs(name), AVG(age) FROM test GROUP BY ABS(name).
   */
  @Test
  public void sql_group_by_function_in_uppercase() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList
                    .of(DSL
                        .named("AVG(integer_value)", dsl.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("ABS(long_value)",
                    dsl.abs(DSL.ref("long_value", LONG))))),
            DSL.named("abs(long_value)", DSL.ref("ABS(long_value)", LONG)),
            DSL.named("AVG(integer_value)", DSL.ref("AVG(integer_value)", DOUBLE))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(alias("AVG(integer_value)",
                    aggregate("AVG", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList
                    .of(alias("ABS(long_value)", function("ABS", qualifiedName("long_value")))),
                emptyList()),
            AstDSL.alias("abs(long_value)", function("abs", qualifiedName("long_value"))),
            AstDSL.alias("AVG(integer_value)", aggregate("AVG", qualifiedName("integer_value"))))
    );
  }

  /**
   * SELECT abs(name), abs(avg(age) FROM test GROUP BY abs(name).
   */
  @Test
  public void sql_expression_over_one_aggregation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList
                    .of(DSL.named("avg(integer_value)",
                        dsl.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("abs(long_value)",
                    dsl.abs(DSL.ref("long_value", LONG))))),
            DSL.named("abs(long_value)", DSL.ref("abs(long_value)", LONG)),
            DSL.named("abs(avg(integer_value)", dsl.abs(DSL.ref("avg(integer_value)", DOUBLE)))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("avg(integer_value)", aggregate("avg", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList
                    .of(alias("abs(long_value)", function("abs", qualifiedName("long_value")))),
                emptyList()),
            AstDSL.alias("abs(long_value)", function("abs", qualifiedName("long_value"))),
            AstDSL.alias("abs(avg(integer_value)",
                function("abs", aggregate("avg", qualifiedName("integer_value")))))
    );
  }

  /**
   * SELECT abs(name), sum(age)-avg(age) FROM test GROUP BY abs(name).
   */
  @Test
  public void sql_expression_over_two_aggregation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList
                    .of(DSL.named("sum(integer_value)",
                            dsl.sum(DSL.ref("integer_value", INTEGER))),
                        DSL.named("avg(integer_value)",
                            dsl.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("abs(long_value)",
                    dsl.abs(DSL.ref("long_value", LONG))))),
            DSL.named("abs(long_value)", DSL.ref("abs(long_value)", LONG)),
            DSL.named("sum(integer_value)-avg(integer_value)",
                dsl.subtract(DSL.ref("sum(integer_value)", INTEGER),
                    DSL.ref("avg(integer_value)", DOUBLE)))),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("sum(integer_value)", aggregate("sum", qualifiedName("integer_value"))),
                    alias("avg(integer_value)", aggregate("avg", qualifiedName("integer_value")))),
                emptyList(),
                ImmutableList
                    .of(alias("abs(long_value)", function("abs", qualifiedName("long_value")))),
                emptyList()),
            AstDSL.alias("abs(long_value)", function("abs", qualifiedName("long_value"))),
            AstDSL.alias("sum(integer_value)-avg(integer_value)",
                function("-", aggregate("sum", qualifiedName("integer_value")),
                    aggregate("avg", qualifiedName("integer_value")))))
    );
  }

  @Test
  public void limit_offset() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.limit(
                LogicalPlanDSL.relation("schema", table),
                1, 1
            ),
            DSL.named("integer_value", DSL.ref("integer_value", INTEGER))
        ),
        AstDSL.project(
            AstDSL.limit(
                AstDSL.relation("schema"),
                1, 1
            ),
            AstDSL.alias("integer_value", qualifiedName("integer_value"))
        )
    );
  }

  /**
   * SELECT COUNT(NAME) FILTER(WHERE age > 1) FROM test.
   * This test is to verify that the aggregator properties are taken
   * when wrapping it to {@link org.opensearch.sql.expression.aggregation.NamedAggregator}
   */
  @Test
  public void named_aggregator_with_condition() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("count(string_value) filter(where integer_value > 1)",
                        dsl.count(DSL.ref("string_value", STRING)).condition(dsl.greater(DSL.ref(
                            "integer_value", INTEGER), DSL.literal(1))))
                ),
                emptyList()
            ),
            DSL.named("count(string_value) filter(where integer_value > 1)", DSL.ref(
                "count(string_value) filter(where integer_value > 1)", INTEGER))
        ),
        AstDSL.project(
            AstDSL.agg(
                AstDSL.relation("schema"),
                ImmutableList.of(
                    alias("count(string_value) filter(where integer_value > 1)", filteredAggregate(
                        "count", qualifiedName("string_value"), function(
                            ">", qualifiedName("integer_value"), intLiteral(1))))),
                emptyList(),
                emptyList(),
                emptyList()
            ),
            AstDSL.alias("count(string_value) filter(where integer_value > 1)", filteredAggregate(
                "count", qualifiedName("string_value"), function(
                    ">", qualifiedName("integer_value"), intLiteral(1))))
        )
    );
  }

  /**
   * stats avg(integer_value) by string_value span(long_value, 10).
   */
  @Test
  public void ppl_stats_by_fieldAndSpan() {
    assertAnalyzeEqual(
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("schema", table),
            ImmutableList.of(
                DSL.named("AVG(integer_value)", dsl.avg(DSL.ref("integer_value", INTEGER)))),
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
  public void parse_relation() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.relation("schema", table),
            ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING))),
            ImmutableList.of(DSL.named("group",
                DSL.parsed(DSL.ref("string_value", STRING), DSL.literal("(?<group>.*)"),
                    DSL.literal("group"))))
        ),
        AstDSL.project(
            AstDSL.parse(
                AstDSL.relation("schema"),
                AstDSL.field("string_value"),
                AstDSL.stringLiteral("(?<group>.*)")),
            AstDSL.alias("string_value", qualifiedName("string_value"))
        ));
  }

  @Test
  public void kmeanns_relation() {
    Map<String, Literal> argumentMap = new HashMap<String, Literal>() {{
        put("centroids", new Literal(3, DataType.INTEGER));
        put("iterations", new Literal(2, DataType.INTEGER));
        put("distance_type", new Literal("COSINE", DataType.STRING));
      }};
    assertAnalyzeEqual(
        new LogicalMLCommons(LogicalPlanDSL.relation("schema", table),
            "kmeans", argumentMap),
        new Kmeans(AstDSL.relation("schema"), argumentMap)
    );
  }

  @Test
  public void ad_batchRCF_relation() {
    Map<String, Literal> argumentMap =
        new HashMap<String, Literal>() {{
            put("shingle_size", new Literal(8, DataType.INTEGER));
          }};
    assertAnalyzeEqual(
        new LogicalAD(LogicalPlanDSL.relation("schema", table), argumentMap),
        new AD(AstDSL.relation("schema"), argumentMap)
    );
  }

  @Test
  public void ad_fitRCF_relation() {
    Map<String, Literal> argumentMap = new HashMap<String, Literal>() {{
        put("shingle_size", new Literal(8, DataType.INTEGER));
        put("time_decay", new Literal(0.0001, DataType.DOUBLE));
        put("time_field", new Literal("timestamp", DataType.STRING));
      }};
    assertAnalyzeEqual(
        new LogicalAD(LogicalPlanDSL.relation("schema", table),
            argumentMap),
        new AD(AstDSL.relation("schema"), argumentMap)
    );
  }

  @Test
  public void table_function_with_named_parameters() {
    assertAnalyzeEqual(
        LogicalPlanDSL.tableFunction(dsl.query_range_function(
            dsl.namedArgument("query", DSL.literal("http_latency")),
            dsl.namedArgument("starttime", DSL.literal(12345)),
            dsl.namedArgument("endtime", DSL.literal(12345)),
            dsl.namedArgument("step", DSL.literal(14))), table),
        AstDSL.tableFunction("prometheus.query_range",
            unresolvedArg("query", stringLiteral("http_latency")),
            unresolvedArg("starttime", intLiteral(12345)),
            unresolvedArg("endtime", intLiteral(12345)),
            unresolvedArg("step", intLiteral(14))));
  }


  @Test
  public void table_function_with_position_parameters() {
    assertAnalyzeEqual(
        LogicalPlanDSL.tableFunction(dsl.query_range_function(
            dsl.namedArgument("query", DSL.literal("http_latency")),
            dsl.namedArgument("starttime", DSL.literal(12345)),
            dsl.namedArgument("endtime", DSL.literal(12345)),
            dsl.namedArgument("step", DSL.literal(14))), table),
        AstDSL.tableFunction("prometheus.query_range",
            unresolvedArg("", stringLiteral("http_latency")),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg(null, intLiteral(14))));
  }

  @Test
  public void table_function_with_position_and_named_parameters() {
    SemanticCheckException exception = assertThrows(SemanticCheckException.class,
        () -> analyze(AstDSL.tableFunction("prometheus.query_range",
            unresolvedArg("query", stringLiteral("http_latency")),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg(null, intLiteral(14)))));
    assertEquals("Arguments should be either passed by name or position", exception.getMessage());
  }

  @Test
  public void table_function_with_no_catalog() {
    SemanticCheckException exception = assertThrows(SemanticCheckException.class,
        () -> analyze(AstDSL.tableFunction("query_range",
            unresolvedArg("query", stringLiteral("http_latency")),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg(null, intLiteral(14)))));
    assertEquals("Catalog not specified along with table function: query_range",
        exception.getMessage());
  }

  @Test
  public void table_function_with_wrong_catalog() {
    SemanticCheckException exception = assertThrows(SemanticCheckException.class,
        () -> analyze(AstDSL.tableFunction("prome.query_range",
            unresolvedArg("query", stringLiteral("http_latency")),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg(null, intLiteral(14)))));
    assertEquals("Catalog: prome not found", exception.getMessage());
  }

  @Test
  public void table_function_with_wrong_table_function() {
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> analyze(AstDSL.tableFunction("prometheus.queryrange",
            unresolvedArg("query", stringLiteral("http_latency")),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg("", intLiteral(12345)),
            unresolvedArg(null, intLiteral(14)))));
    assertEquals("unsupported function name: queryrange", exception.getMessage());
  }


}

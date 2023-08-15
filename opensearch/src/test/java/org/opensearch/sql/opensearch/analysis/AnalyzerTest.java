/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.analysis;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.analysis.NestedAnalyzer.isNestedFunction;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.nestedAllTupleFields;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ScoreFunction;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;

class AnalyzerTest extends AnalyzerTestBase {

  @Test
  public void analyze_filter_visit_score_function() {
    UnresolvedPlan unresolvedPlan = AstDSL.filter(
        AstDSL.relation("schema"),
        new ScoreFunction(
            AstDSL.function("match_phrase_prefix",
                AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                AstDSL.unresolvedArg("query", stringLiteral("search query")),
                AstDSL.unresolvedArg("boost", stringLiteral("3"))
            ), AstDSL.doubleLiteral(1.0))
    );
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.match_phrase_prefix(
                DSL.namedArgument("field", "field_value1"),
                DSL.namedArgument("query", "search query"),
                DSL.namedArgument("boost", "3.0")
            )
        ),
        unresolvedPlan
    );

    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunctions.OpenSearchFunction relevanceQuery =
        (OpenSearchFunctions.OpenSearchFunction)((LogicalFilter) logicalPlan).getCondition();
    assertEquals(true, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_without_score_function() {
    UnresolvedPlan unresolvedPlan = AstDSL.filter(
        AstDSL.relation("schema"),
        AstDSL.function("match_phrase_prefix",
            AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
            AstDSL.unresolvedArg("query", stringLiteral("search query")),
            AstDSL.unresolvedArg("boost", stringLiteral("3"))
        )
    );
    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.match_phrase_prefix(
                DSL.namedArgument("field", "field_value1"),
                DSL.namedArgument("query", "search query"),
                DSL.namedArgument("boost", "3")
            )
        ),
        unresolvedPlan
    );

    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunctions.OpenSearchFunction relevanceQuery =
        (OpenSearchFunctions.OpenSearchFunction)((LogicalFilter) logicalPlan).getCondition();
    assertEquals(false, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_score_function_with_double_boost() {
    UnresolvedPlan unresolvedPlan = AstDSL.filter(
        AstDSL.relation("schema"),
        new ScoreFunction(
            AstDSL.function("match_phrase_prefix",
                AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                AstDSL.unresolvedArg("query", stringLiteral("search query")),
                AstDSL.unresolvedArg("slop", stringLiteral("3"))
            ), new Literal(3.0, DataType.DOUBLE)
        )
    );

    assertAnalyzeEqual(
        LogicalPlanDSL.filter(
            LogicalPlanDSL.relation("schema", table),
            DSL.match_phrase_prefix(
                DSL.namedArgument("field", "field_value1"),
                DSL.namedArgument("query", "search query"),
                DSL.namedArgument("slop", "3"),
                DSL.namedArgument("boost", "3.0")
            )
        ),
        unresolvedPlan
    );

    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunctions.OpenSearchFunction relevanceQuery =
        (OpenSearchFunctions.OpenSearchFunction)((LogicalFilter) logicalPlan).getCondition();
    assertEquals(true, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_score_function_with_unsupported_boost_SemanticCheckException() {
    UnresolvedPlan unresolvedPlan = AstDSL.filter(
        AstDSL.relation("schema"),
        new ScoreFunction(
            AstDSL.function("match_phrase_prefix",
                AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                AstDSL.unresolvedArg("query", stringLiteral("search query")),
                AstDSL.unresolvedArg("boost", stringLiteral("3"))
            ), AstDSL.stringLiteral("3.0")
        )
    );
    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () -> analyze(unresolvedPlan));
    assertEquals(
        "Expected boost type 'DOUBLE' but got 'STRING'",
        exception.getMessage());
  }

  @Test
  public void project_nested_field_arg() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)
            )
        );

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING)),
                null)
        );

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING)))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.info)",
                function("nested", qualifiedName("message", "info")), null)
        )
    );

    assertTrue(isNestedFunction(DSL.nested(DSL.ref("message.info", STRING))));
    assertFalse(isNestedFunction(DSL.literal("fieldA")));
    assertFalse(isNestedFunction(DSL.match(DSL.namedArgument("field", literal("message")))));
  }

  @Test
  public void sort_with_nested_all_tuple_fields_throws_exception() {
    assertThrows(UnsupportedOperationException.class, () -> analyze(
        AstDSL.project(
            AstDSL.sort(
                AstDSL.relation("schema"),
                field(nestedAllTupleFields("message"))
            ),
            AstDSL.alias("nested(message.*)",
                nestedAllTupleFields("message"))
        )
    ));
  }

  @Test
  public void filter_with_nested_all_tuple_fields_throws_exception() {
    assertThrows(UnsupportedOperationException.class, () -> analyze(
        AstDSL.project(
            AstDSL.filter(
                AstDSL.relation("schema"),
                AstDSL.function("=", nestedAllTupleFields("message"), AstDSL.intLiteral(1))),
            AstDSL.alias("nested(message.*)",
                nestedAllTupleFields("message"))
        )
    ));
  }


  @Test
  public void project_nested_field_star_arg() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)
            )
        );

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING)))
        );

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING)))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.*)",
                nestedAllTupleFields("message"))
        )
    );
  }

  @Test
  public void project_nested_field_star_arg_with_another_nested_function() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)
            ),
            Map.of(
                "field", new ReferenceExpression("comment.data", STRING),
                "path", new ReferenceExpression("comment", STRING)
            )
        );

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING))),
            new NamedExpression("nested(comment.data)",
                DSL.nested(DSL.ref("comment.data", STRING)))
        );

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("nested(comment.data)",
                DSL.nested(DSL.ref("comment.data", STRING)))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.*)",
                nestedAllTupleFields("message")),
            AstDSL.alias("nested(comment.*)",
                nestedAllTupleFields("comment"))
        )
    );
  }

  @Test
  public void project_nested_field_star_arg_with_another_field() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)
            )
        );

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING))),
            new NamedExpression("comment.data",
                DSL.ref("comment.data", STRING))
        );

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("comment.data",
                DSL.ref("comment.data", STRING))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.*)",
                nestedAllTupleFields("message")),
            AstDSL.alias("comment.data",
                field("comment.data"))
        )
    );
  }

  @Test
  public void project_nested_field_star_arg_with_highlight() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)
            )
        );

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("highlight(fieldA)",
                new HighlightExpression(DSL.literal("fieldA")))
        );

    Map<String, Literal> highlightArgs = new HashMap<>();

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.highlight(LogicalPlanDSL.relation("schema", table),
                    DSL.literal("fieldA"), highlightArgs),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("highlight(fieldA)",
                new HighlightExpression(DSL.literal("fieldA")))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.*)",
                nestedAllTupleFields("message")),
            AstDSL.alias("highlight(fieldA)",
                new HighlightFunction(AstDSL.stringLiteral("fieldA"), highlightArgs))
        )
    );
  }

  @Test
  public void project_nested_field_and_path_args() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)
            )
        );

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING), DSL.ref("message", STRING)),
                null)
        );

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING), DSL.ref("message", STRING)))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.info)",
                function(
                    "nested",
                    qualifiedName("message", "info"),
                    qualifiedName("message")
                ),
                null
            )
        )
    );
  }

  @Test
  public void project_nested_deep_field_arg() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info.id", STRING),
                "path", new ReferenceExpression("message.info", STRING)
            )
        );

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info.id)",
                DSL.nested(DSL.ref("message.info.id", STRING)),
                null)
        );

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info.id)",
                DSL.nested(DSL.ref("message.info.id", STRING)))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.info.id)",
                function("nested", qualifiedName("message", "info", "id")), null)
        )
    );
  }

  @Test
  public void project_multiple_nested() {
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)
            ),
            Map.of(
                "field", new ReferenceExpression("comment.data", STRING),
                "path", new ReferenceExpression("comment", STRING)
            )
        );

    List<NamedExpression> projectList =
        List.of(
            new NamedExpression(
                "nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING)),
                null),
            new NamedExpression(
                "nested(comment.data)",
                DSL.nested(DSL.ref("comment.data", STRING)),
                null)
        );

    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.nested(
                LogicalPlanDSL.relation("schema", table),
                nestedArgs,
                projectList),
            DSL.named("nested(message.info)",
                DSL.nested(DSL.ref("message.info", STRING))),
            DSL.named("nested(comment.data)",
                DSL.nested(DSL.ref("comment.data", STRING)))
        ),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.alias("nested(message.info)",
                function("nested", qualifiedName("message", "info")), null),
            AstDSL.alias("nested(comment.data)",
                function("nested", qualifiedName("comment", "data")), null)
        )
    );
  }

  @Test
  public void project_nested_invalid_field_throws_exception() {
    var exception = assertThrows(
        IllegalArgumentException.class,
        () -> analyze(AstDSL.projectWithArg(
                AstDSL.relation("schema"),
                AstDSL.defaultFieldsArgs(),
                AstDSL.alias("message",
                    function("nested", qualifiedName("message")), null)
            )
        )
    );
    assertEquals(exception.getMessage(), "Illegal nested field name: message");
  }

  @Test
  public void project_nested_invalid_arg_type_throws_exception() {
    var exception = assertThrows(
        IllegalArgumentException.class,
        () -> analyze(AstDSL.projectWithArg(
                AstDSL.relation("schema"),
                AstDSL.defaultFieldsArgs(),
                AstDSL.alias("message",
                    function("nested", stringLiteral("message")), null)
            )
        )
    );
    assertEquals(exception.getMessage(), "Illegal nested field name: message");
  }

  @Test
  public void project_nested_no_args_throws_exception() {
    var exception = assertThrows(
        IllegalArgumentException.class,
        () -> analyze(AstDSL.projectWithArg(
                AstDSL.relation("schema"),
                AstDSL.defaultFieldsArgs(),
                AstDSL.alias("message",
                    function("nested"), null)
            )
        )
    );
    assertEquals(exception.getMessage(),
        "on nested object only allowed 2 parameters (field,path) or 1 parameter (field)"
    );
  }

  @Test
  public void project_nested_too_many_args_throws_exception() {
    var exception = assertThrows(
        IllegalArgumentException.class,
        () -> analyze(AstDSL.projectWithArg(
                AstDSL.relation("schema"),
                AstDSL.defaultFieldsArgs(),
                AstDSL.alias("message",
                    function("nested",
                        stringLiteral("message.info"),
                        stringLiteral("message"),
                        stringLiteral("message")),
                    null)
            )
        )
    );
    assertEquals(exception.getMessage(),
        "on nested object only allowed 2 parameters (field,path) or 1 parameter (field)"
    );
  }

  /**
   * Ensure Nested function falls back to legacy engine when used in GROUP BY clause.
   * TODO Remove this test when support is added.
   */
  @Test
  public void nested_group_by_clause_throws_syntax_exception() {
    SyntaxCheckException exception = assertThrows(SyntaxCheckException.class,
        () -> analyze(
            AstDSL.project(
                AstDSL.agg(
                    AstDSL.relation("schema"),
                    emptyList(),
                    emptyList(),
                    ImmutableList.of(alias("nested(message.info)",
                        function("nested",
                            qualifiedName("message", "info")))),
                    emptyList()
                )))
    );
    assertEquals("Falling back to legacy engine. Nested function is not supported in WHERE,"
            + " GROUP BY, and HAVING clauses.",
        exception.getMessage());
  }
}

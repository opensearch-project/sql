/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.ScoreFunction;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.OpenSearchFunction;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;

@ExtendWith(MockitoExtension.class)
class OpenSearchAnalyzerTest extends AnalyzerTestBase {

  @Mock private BuiltinFunctionRepository builtinFunctionRepository;

  @Override
  protected ExpressionAnalyzer expressionAnalyzer() {
    return new ExpressionAnalyzer(builtinFunctionRepository);
  }

  @BeforeEach
  private void setup() {
    this.expressionAnalyzer = expressionAnalyzer();
    this.analyzer = analyzer(this.expressionAnalyzer, dataSourceService);
  }

  @Test
  public void analyze_filter_visit_score_function() {

    // setup
    OpenSearchFunction scoreFunction =
        new OpenSearchFunction(new FunctionName("match_phrase_prefix"), List.of());
    when(builtinFunctionRepository.compile(any(), any(), any())).thenReturn(scoreFunction);

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

    // test
    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunction relevanceQuery =
        (OpenSearchFunction) ((LogicalFilter) logicalPlan).getCondition();

    // verify
    assertEquals(true, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_score_function_without_boost() {

    // setup
    OpenSearchFunction scoreFunction =
        new OpenSearchFunction(new FunctionName("match_phrase_prefix"), List.of());
    when(builtinFunctionRepository.compile(any(), any(), any())).thenReturn(scoreFunction);

    UnresolvedPlan unresolvedPlan =
        AstDSL.filter(
            AstDSL.relation("schema"),
            new ScoreFunction(
                AstDSL.function(
                    "match_phrase_prefix",
                    AstDSL.unresolvedArg("field", stringLiteral("field_value1")),
                    AstDSL.unresolvedArg("query", stringLiteral("search query"))),
                AstDSL.doubleLiteral(1.0)));

    // test
    LogicalPlan logicalPlan = analyze(unresolvedPlan);
    OpenSearchFunction relevanceQuery =
        (OpenSearchFunction) ((LogicalFilter) logicalPlan).getCondition();

    // verify
    assertEquals(true, relevanceQuery.isScoreTracked());
  }

  @Test
  public void analyze_filter_visit_score_function_with_unsupported_boost_SemanticCheckException() {
    // setup
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

    // Test
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> analyze(unresolvedPlan));

    // Verify
    assertEquals("Expected boost type 'DOUBLE' but got 'STRING'", exception.getMessage());
  }

  @Test
  public void analyze_relevance_field_list() {
    LinkedHashMap tuple = new LinkedHashMap(Map.of("Title", 1.0F, "Body", 4.2F, "Tags", 1.5F));

    UnresolvedExpression unresolvedPlan = new RelevanceFieldList(tuple);
    Expression relevanceFieldList = unresolvedPlan.accept(expressionAnalyzer, analysisContext);
    assertEquals(STRUCT, relevanceFieldList.type());
    assertTrue(relevanceFieldList.valueOf() instanceof ExprTupleValue);
    assertEquals(
        tuple.get("Tags"), relevanceFieldList.valueOf().tupleValue().get("Tags").floatValue());
    assertEquals(
        tuple.get("Body"), relevanceFieldList.valueOf().tupleValue().get("Body").floatValue());
    assertEquals(
        tuple.get("Title"), relevanceFieldList.valueOf().tupleValue().get("Title").floatValue());
  }
}

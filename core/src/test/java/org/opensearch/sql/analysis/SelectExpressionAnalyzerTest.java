/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;

@ExtendWith(MockitoExtension.class)
public class SelectExpressionAnalyzerTest extends AnalyzerTestBase {

  @Mock private ExpressionReferenceOptimizer optimizer;

  @Test
  public void named_expression() {
    assertAnalyzeEqual(
        DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
        AstDSL.alias("integer_value", AstDSL.qualifiedName("integer_value")));
  }

  @Test
  public void named_expression_with_alias() {
    assertAnalyzeEqual(
        DSL.named("integer_value", DSL.ref("integer_value", INTEGER), "int"),
        AstDSL.alias("integer_value", AstDSL.qualifiedName("integer_value"), "int"));
  }

  @Test
  public void field_name_with_qualifier() {
    analysisContext.peek().define(new Symbol(Namespace.INDEX_NAME, "index_alias"), STRUCT);
    assertAnalyzeEqual(
        DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
        AstDSL.alias(
            "integer_alias.integer_value", AstDSL.qualifiedName("index_alias", "integer_value")));
  }

  @Test
  public void field_name_with_qualifier_quoted() {
    analysisContext.peek().define(new Symbol(Namespace.INDEX_NAME, "index_alias"), STRUCT);
    assertAnalyzeEqual(
        DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
        AstDSL.alias(
            "`integer_alias`.integer_value", // qualifier in SELECT is quoted originally
            AstDSL.qualifiedName("index_alias", "integer_value")));
  }

  @Test
  public void field_name_in_expression_with_qualifier() {
    analysisContext.peek().define(new Symbol(Namespace.INDEX_NAME, "index_alias"), STRUCT);
    assertAnalyzeEqual(
        DSL.named("abs(index_alias.integer_value)", DSL.abs(DSL.ref("integer_value", INTEGER))),
        AstDSL.alias(
            "abs(index_alias.integer_value)",
            AstDSL.function("abs", AstDSL.qualifiedName("index_alias", "integer_value"))));
  }

  protected List<NamedExpression> analyze(UnresolvedExpression unresolvedExpression) {
    doAnswer(invocation -> ((NamedExpression) invocation.getArgument(0)).getDelegated())
        .when(optimizer)
        .optimize(any(), any());
    return new SelectExpressionAnalyzer(expressionAnalyzer)
        .analyze(List.of(unresolvedExpression), analysisContext, optimizer);
  }

  protected void assertAnalyzeEqual(
      NamedExpression expected, UnresolvedExpression unresolvedExpression) {
    assertEquals(List.of(expected), analyze(unresolvedExpression));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.expression.NamedExpression;

class NamedExpressionAnalyzerTest extends AnalyzerTestBase {
  @Test
  void visit_named_select_item() {
    Alias alias = AstDSL.alias("integer_value", AstDSL.qualifiedName("integer_value"));

    NamedExpressionAnalyzer analyzer = new NamedExpressionAnalyzer(expressionAnalyzer);

    NamedExpression analyze = analyzer.analyze(alias, analysisContext);
    assertEquals("integer_value", analyze.getNameOrAlias());
  }

  @Test
  void visit_highlight() {
    Map<String, Literal> args = new HashMap<>();
    Alias alias =
        AstDSL.alias(
            "highlight(fieldA)", new HighlightFunction(AstDSL.stringLiteral("fieldA"), args));
    NamedExpressionAnalyzer analyzer = new NamedExpressionAnalyzer(expressionAnalyzer);

    NamedExpression analyze = analyzer.analyze(alias, analysisContext);
    assertEquals("highlight(fieldA)", analyze.getNameOrAlias());
  }
}

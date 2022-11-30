/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.PositionFunction;
import org.opensearch.sql.expression.NamedExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {AnalyzerTestBase.class})
class NamedExpressionAnalyzerTest extends AnalyzerTestBase {
  @Test
  void visit_named_select_item() {
    Alias alias = AstDSL.alias("integer_value", AstDSL.qualifiedName("integer_value"));

    NamedExpressionAnalyzer analyzer =
        new NamedExpressionAnalyzer(expressionAnalyzer);

    NamedExpression analyze = analyzer.analyze(alias, analysisContext);
    assertEquals("integer_value", analyze.getNameOrAlias());
  }

  @Test
  void visit_highlight() {
    Map<String, Literal> args = new HashMap<>();
    Alias alias = AstDSL.alias("highlight(fieldA)",
        new HighlightFunction(
            AstDSL.stringLiteral("fieldA"), args));
    NamedExpressionAnalyzer analyzer =
        new NamedExpressionAnalyzer(expressionAnalyzer);

    NamedExpression analyze = analyzer.analyze(alias, analysisContext);
    assertEquals("highlight(fieldA)", analyze.getNameOrAlias());
  }

  @Test
  void visit_position() {
    Alias alias = AstDSL.alias("position(fieldA IN fieldB)",
        new PositionFunction(AstDSL.stringLiteral("fieldA"), AstDSL.stringLiteral("fieldB")));
    NamedExpressionAnalyzer analyzer = new NamedExpressionAnalyzer(expressionAnalyzer);

    NamedExpression analyze = analyzer.analyze(alias, analysisContext);
    assertEquals("position(fieldA IN fieldB)", analyze.getNameOrAlias());
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTestBase.class})
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
    Alias alias = AstDSL.alias("highlight(fieldA)",
        new HighlightFunction(AstDSL.stringLiteral("fieldA")));
    NamedExpressionAnalyzer analyzer =
        new NamedExpressionAnalyzer(expressionAnalyzer);

    NamedExpression analyze = analyzer.analyze(alias, analysisContext);
    assertEquals("highlight(fieldA)", analyze.getNameOrAlias());
  }
}

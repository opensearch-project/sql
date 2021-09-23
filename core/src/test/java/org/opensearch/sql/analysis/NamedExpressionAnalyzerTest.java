/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
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
  void visit_span() {
    NamedExpressionAnalyzer analyzer =
        new NamedExpressionAnalyzer(expressionAnalyzer);

    Span span = AstDSL.span(AstDSL.qualifiedName("integer_value"), AstDSL.intLiteral(
        1), SpanUnit.NONE);
    NamedExpression named = analyzer.analyze(span, analysisContext);
    assertEquals(span.toString(), named.getNameOrAlias());
  }
}

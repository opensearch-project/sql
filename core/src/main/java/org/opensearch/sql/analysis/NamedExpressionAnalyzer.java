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

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;

/**
 * Analyze the Alias node in the {@link AnalysisContext} to construct the list of
 * {@link NamedExpression}.
 */
@RequiredArgsConstructor
public class NamedExpressionAnalyzer extends
    AbstractNodeVisitor<NamedExpression, AnalysisContext> {
  private final ExpressionAnalyzer expressionAnalyzer;

  /**
   * Analyze Select fields.
   */
  public NamedExpression analyze(UnresolvedExpression expression,
                                       AnalysisContext analysisContext) {
    return expression.accept(this, analysisContext);
  }

  @Override
  public NamedExpression visitAlias(Alias node, AnalysisContext context) {
    return DSL.named(
        unqualifiedNameIfFieldOnly(node, context),
        node.getDelegated().accept(expressionAnalyzer, context),
        node.getAlias());
  }

  private String unqualifiedNameIfFieldOnly(Alias node, AnalysisContext context) {
    UnresolvedExpression selectItem = node.getDelegated();
    if (selectItem instanceof QualifiedName) {
      QualifierAnalyzer qualifierAnalyzer = new QualifierAnalyzer(context);
      return qualifierAnalyzer.unqualified((QualifiedName) selectItem);
    }
    return node.getName();
  }
}

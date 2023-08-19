/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.expression;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.opensearch.analysis.OpenSearchAbstractNodeVisitor;

/**
 * Expression node of Score function. Score takes a relevance-search expression as an argument and
 * returns it
 */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class ScoreFunction extends OpenSearchUnresolvedExpression {
  private final OpenSearchUnresolvedExpression relevanceQuery;
  private final Literal relevanceFieldWeight;

  @Override
  public <T, C> T accept(OpenSearchAbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitScoreFunction(this, context);
  }

  @Override
  public List<OpenSearchUnresolvedExpression> getChild() {
    return List.of(relevanceQuery);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.aggregation;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.aggregation.AggregationState;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.WindowFunctionExpression;
import org.opensearch.sql.expression.window.frame.PeerRowsWindowFrame;
import org.opensearch.sql.expression.window.frame.WindowFrame;

/** Aggregate function adapter that adapts Aggregator for window operator use. */
@EqualsAndHashCode
@RequiredArgsConstructor
public class AggregateWindowFunction implements WindowFunctionExpression {

  private final Aggregator<AggregationState> aggregator;
  private AggregationState state;

  @Override
  public WindowFrame createWindowFrame(WindowDefinition definition) {
    return new PeerRowsWindowFrame(definition);
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    PeerRowsWindowFrame frame = (PeerRowsWindowFrame) valueEnv;
    if (frame.isNewPartition()) {
      state = aggregator.create();
    }

    List<ExprValue> peers = frame.next();
    for (ExprValue peer : peers) {
      state = aggregator.iterate(peer.bindingTuples(), state);
    }
    return state.result();
  }

  @Override
  public ExprType type() {
    return aggregator.type();
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return aggregator.accept(visitor, context);
  }

  @Override
  public String toString() {
    return aggregator.toString();
  }
}

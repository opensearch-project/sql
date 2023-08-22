/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ExpressionNodeVisitor;

/**
 * NamedAggregator expression that represents expression with name. Please see more details in
 * associated unresolved expression operator<br>
 * {@link org.opensearch.sql.ast.expression.Alias}.
 */
@EqualsAndHashCode(callSuper = false)
public class NamedAggregator extends Aggregator<AggregationState> {

  /** Aggregator name. */
  private final String name;

  /** Aggregator that being named. */
  @Getter private final Aggregator<AggregationState> delegated;

  /**
   * NamedAggregator. The aggregator properties {@link #condition} and {@link #distinct} are
   * inherited by named aggregator to avoid errors introduced by the property inconsistency.
   *
   * @param name name
   * @param delegated delegated
   */
  public NamedAggregator(String name, Aggregator<AggregationState> delegated) {
    super(delegated.getFunctionName(), delegated.getArguments(), delegated.returnType);
    this.name = name;
    this.delegated = delegated;
    this.condition = delegated.condition;
    this.distinct = delegated.distinct;
  }

  @Override
  public AggregationState create() {
    return delegated.create();
  }

  @Override
  protected AggregationState iterate(ExprValue value, AggregationState state) {
    return delegated.iterate(value, state);
  }

  /**
   * Get expression name using name or its alias (if it's present).
   *
   * @return expression name
   */
  public String getName() {
    return name;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitNamedAggregator(this, context);
  }

  @Override
  public String toString() {
    return getName();
  }
}

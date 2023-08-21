/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionImplementation;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Aggregator which will iterate on the {@link BindingTuple}s to aggregate the result. The
 * Aggregator is not well fit into Expression, because it has side effect. But we still want to make
 * it implement {@link Expression} interface to make {@link ExpressionAnalyzer} easier.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public abstract class Aggregator<S extends AggregationState>
    implements FunctionImplementation, Expression {
  @Getter private final FunctionName functionName;
  @Getter private final List<Expression> arguments;
  protected final ExprCoreType returnType;

  @Setter
  @Getter
  @Accessors(fluent = true)
  protected Expression condition;

  @Setter
  @Getter
  @Accessors(fluent = true)
  protected Boolean distinct = false;

  /** Create an {@link AggregationState} which will be used for aggregation. */
  public abstract S create();

  /**
   * Iterate on {@link ExprValue}.
   *
   * @param value {@link ExprValue}
   * @param state {@link AggregationState}
   * @return {@link AggregationState}
   */
  protected abstract S iterate(ExprValue value, S state);

  /**
   * Let the aggregator iterate on the {@link BindingTuple} To filter out ExprValues that are
   * missing, null or cannot satisfy {@link #condition} Before the specific aggregator iterating
   * ExprValue in the tuple.
   *
   * @param tuple {@link BindingTuple}
   * @param state {@link AggregationState}
   * @return {@link AggregationState}
   */
  public S iterate(BindingTuple tuple, S state) {
    ExprValue value = getArguments().get(0).valueOf(tuple);
    if (value.isNull() || value.isMissing() || !conditionValue(tuple)) {
      return state;
    }
    return iterate(value, state);
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new ExpressionEvaluationException(
        String.format("can't evaluate on aggregator: %s", functionName));
  }

  @Override
  public ExprType type() {
    return returnType;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitAggregator(this, context);
  }

  /** Util method to get value of condition in aggregation filter. */
  public boolean conditionValue(BindingTuple tuple) {
    if (condition == null) {
      return true;
    }
    return ExprValueUtils.getBooleanValue(condition.valueOf(tuple));
  }
}

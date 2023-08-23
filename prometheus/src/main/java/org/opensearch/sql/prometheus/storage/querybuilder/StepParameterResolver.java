/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.querybuilder;

import java.util.List;
import java.util.Optional;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;

/** This class resolves step parameter required for query_range api of prometheus. */
@NoArgsConstructor
public class StepParameterResolver {

  /**
   * Extract step from groupByList or apply heuristic arithmetic on endTime and startTime.
   *
   * @param startTime startTime.
   * @param endTime endTime.
   * @param groupByList groupByList.
   * @return Step String.
   */
  public static String resolve(
      @NonNull Long startTime, @NonNull Long endTime, List<NamedExpression> groupByList) {
    Optional<SpanExpression> spanExpression = getSpanExpression(groupByList);
    if (spanExpression.isPresent()) {
      if (StringUtils.isEmpty(spanExpression.get().getUnit().getName())) {
        throw new RuntimeException("Missing TimeUnit in the span expression");
      } else {
        return spanExpression.get().getValue().toString()
            + spanExpression.get().getUnit().getName();
      }
    } else {
      return Math.max((endTime - startTime) / 250, 1) + "s";
    }
  }

  private static Optional<SpanExpression> getSpanExpression(
      List<NamedExpression> namedExpressionList) {
    if (namedExpressionList == null) {
      return Optional.empty();
    }
    return namedExpressionList.stream()
        .filter(expression -> expression.getDelegated() instanceof SpanExpression)
        .map(expression -> (SpanExpression) expression.getDelegated())
        .findFirst();
  }
}

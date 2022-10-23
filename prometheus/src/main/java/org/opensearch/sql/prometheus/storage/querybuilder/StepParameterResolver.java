/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.querybuilder;

import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;

public class StepParameterResolver {

  /**
   * Extract step from groupByList or apply heuristic arithmetic
   * on endTime and startTime.
   *
   *
   * @param startTime startTime.
   * @param endTime endTime.
   * @param groupByList groupByList.
   * @return Step String.
   */
  public String resolve(@NonNull Long startTime, @NonNull Long endTime,
                        List<NamedExpression> groupByList) {
    return getSpanExpression(groupByList).map(
            expression -> expression.getValue().toString() + expression.getUnit().getName())
        .orElse(Math.max((endTime - startTime) / 250, 1) + "s");
  }

  private Optional<SpanExpression> getSpanExpression(List<NamedExpression> groupByList) {
    if (groupByList == null) {
      return Optional.empty();
    }
    return groupByList.stream().map(NamedExpression::getDelegated)
        .filter(delegated -> delegated instanceof SpanExpression)
        .map(delegated -> (SpanExpression) delegated)
        .findFirst();
  }

}
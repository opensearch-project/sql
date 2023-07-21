/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.querybuilder;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.span.SpanExpression;

/**
 * This class builds aggregation query for the given stats commands.
 * In the generated query a placeholder(%s) is added in place of metric selection query
 * and later replaced by metric selection query.
 */
@NoArgsConstructor
public class AggregationQueryBuilder {

  private static final Set<String> allowedStatsFunctions = Set.of(
      BuiltinFunctionName.MAX.getName().getFunctionName(),
      BuiltinFunctionName.MIN.getName().getFunctionName(),
      BuiltinFunctionName.COUNT.getName().getFunctionName(),
      BuiltinFunctionName.SUM.getName().getFunctionName(),
      BuiltinFunctionName.AVG.getName().getFunctionName()
  );


  /**
   * Build Aggregation query from series selector query from expression.
   *
   * @return query string.
   */
  public static String build(List<NamedAggregator> namedAggregatorList,
                      List<NamedExpression> groupByList) {

    if (namedAggregatorList.size() > 1) {
      throw new RuntimeException(
          "Prometheus Catalog doesn't multiple aggregations in stats command");
    }

    if (!allowedStatsFunctions
        .contains(namedAggregatorList.get(0).getFunctionName().getFunctionName())) {
      throw new RuntimeException(String.format(
          "Prometheus Catalog only supports %s aggregations.", allowedStatsFunctions));
    }

    StringBuilder aggregateQuery = new StringBuilder();
    aggregateQuery.append(namedAggregatorList.get(0).getFunctionName().getFunctionName())
        .append(" ");

    if (groupByList != null && !groupByList.isEmpty()) {
      groupByList = groupByList.stream()
          .filter(expression -> !(expression.getDelegated() instanceof SpanExpression))
          .collect(Collectors.toList());
      if (groupByList.size() > 0) {
        aggregateQuery.append("by(");
        aggregateQuery.append(
            groupByList.stream()
                .filter(expression -> expression.getDelegated() instanceof ReferenceExpression)
                .map(expression -> ((ReferenceExpression) expression.getDelegated()).getAttr())
                .collect(Collectors.joining(", ")));
        aggregateQuery.append(")");
      }
    }
    aggregateQuery
        .append(" (")
        .append(namedAggregatorList.get(0).getFunctionName().getFunctionName())
        .append("_over_time")
        .append("(%s))");
    return aggregateQuery.toString();
  }

}

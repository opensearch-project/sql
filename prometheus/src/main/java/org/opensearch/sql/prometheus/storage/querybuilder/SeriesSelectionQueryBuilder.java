/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.querybuilder;


import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;

import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * This class builds metric selection query from the filter condition
 * and metric name.
 */
@NoArgsConstructor
public class SeriesSelectionQueryBuilder {


  /**
   * Build Prometheus series selector query from expression.
   *
   * @param filterCondition expression.
   * @return query string
   */
  public static String build(String metricName, Expression filterCondition) {
    if (filterCondition != null) {
      SeriesSelectionExpressionNodeVisitor seriesSelectionExpressionNodeVisitor
          = new SeriesSelectionExpressionNodeVisitor();
      String selectorQuery = filterCondition.accept(seriesSelectionExpressionNodeVisitor, null);
      if (selectorQuery != null) {
        return metricName + "{" + selectorQuery + "}";
      }
    }
    return metricName;
  }

  static class SeriesSelectionExpressionNodeVisitor extends ExpressionNodeVisitor<String, Object> {
    @Override
    public String visitFunction(FunctionExpression func, Object context) {
      if (func.getFunctionName().getFunctionName().equals("and")) {
        return func.getArguments().stream()
            .map(arg -> visitFunction((FunctionExpression) arg, context))
            .filter(StringUtils::isNotEmpty)
            .collect(Collectors.joining(" , "));
      } else if (func.getFunctionName().getFunctionName().contains("=")) {
        ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
        if (!ref.getAttr().equals(TIMESTAMP)) {
          return func.getArguments().get(0)
              + func.getFunctionName().getFunctionName()
              + func.getArguments().get(1);
        } else {
          return null;
        }
      } else {
        throw new RuntimeException(
            String.format("Prometheus Catalog doesn't support %s in where command.",
                func.getFunctionName().getFunctionName()));
      }
    }
  }

}

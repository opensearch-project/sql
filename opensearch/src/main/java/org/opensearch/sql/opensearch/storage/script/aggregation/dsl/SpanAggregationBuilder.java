/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;

/**
 * Span Aggregation Builder.
 */
public class SpanAggregationBuilder {
  /**
   * Build corresponding aggregation builder for span aggregation.
   * For general span aggregation with no unit:
   * build {@link HistogramAggregationBuilder}
   * For time span aggregation with time span unit:
   * build {@link DateHistogramAggregationBuilder}
   */
  public AggregationBuilder build(NamedExpression namedExpression) {
    SpanExpression spanExpr = (SpanExpression) namedExpression.getDelegated();
    return makeBuilder(namedExpression.getNameOrAlias(), spanExpr.getField()
        .toString(), spanExpr.getValue().valueOf(null).doubleValue(), spanExpr.getUnit());
  }

  private AggregationBuilder makeBuilder(
      String name, String field, Double value, SpanUnit unit) {
    switch (unit) {
      case NONE:
        return new HistogramAggregationBuilder(name)
            .field(field)
            .interval(value);
      case UNKNOWN:
        throw new IllegalStateException("Invalid span unit");
      default:
        return makeDateHistogramBuilder(name, field, value.intValue(), unit);
    }
  }

  private DateHistogramAggregationBuilder makeDateHistogramBuilder(
      String name, String field, Integer value, SpanUnit unit) {
    String spanValue = value + unit.getName();
    switch (unit) {
      case MILLISECOND:
      case MS:
      case SECOND:
      case S:
      case MINUTE:
      case m:
      case HOUR:
      case H:
      case DAY:
      case D:
        return new DateHistogramAggregationBuilder(name)
            .field(field)
            .fixedInterval(new DateHistogramInterval(spanValue));
      default:
        return new DateHistogramAggregationBuilder(name)
            .field(field)
            .calendarInterval(new DateHistogramInterval(spanValue));
    }
  }

}

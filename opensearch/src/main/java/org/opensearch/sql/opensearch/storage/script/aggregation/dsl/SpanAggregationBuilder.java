/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
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
        .toString(), spanExpr.getValue().valueOf(null).stringValue(), spanExpr.getUnit());
  }

  private AggregationBuilder makeBuilder(
      String name, String field, String value, SpanUnit unit) {
    switch (unit) {
      case NONE:
        return new HistogramAggregationBuilder(name)
            .field(field)
            .interval(Double.parseDouble(value));
      case UNKNOWN:
        throw new IllegalStateException("Invalid span unit");
      default:
        return makeDateHistogramBuilder(name, field, value, unit);
    }
  }

  private DateHistogramAggregationBuilder makeDateHistogramBuilder(
      String name, String field, String value, SpanUnit unit) {
    String spanValue = value + unit.getName();
    switch (unit) {
      case MICROSECOND:
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

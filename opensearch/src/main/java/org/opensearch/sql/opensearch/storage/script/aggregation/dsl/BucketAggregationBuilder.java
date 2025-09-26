/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.opensearch.storage.script.aggregation.AggregationQueryBuilder.AGGREGATION_BUCKET_SIZE;

import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.storage.serde.ExpressionSerializer;

/** Bucket Aggregation Builder. */
public class BucketAggregationBuilder {

  private final AggregationBuilderHelper helper;

  public BucketAggregationBuilder(ExpressionSerializer serializer) {
    this.helper = new AggregationBuilderHelper(serializer);
  }

  /** Build the ValuesSourceAggregationBuilder. */
  public ValuesSourceAggregationBuilder<?> build(NamedExpression expr) {
    if (expr.getDelegated() instanceof SpanExpression) {
      SpanExpression spanExpr = (SpanExpression) expr.getDelegated();
      return buildHistogram(
          expr.getName(),
          spanExpr.getField().toString(),
          spanExpr.getValue().valueOf().doubleValue(),
          spanExpr.getUnit());
    } else {
      TermsAggregationBuilder sourceBuilder = new TermsAggregationBuilder(expr.getName());
      sourceBuilder.size(AGGREGATION_BUCKET_SIZE);
      sourceBuilder.order(BucketOrder.key(true));
      // Time types values are converted to LONG in ExpressionAggregationScript::execute
      if ((expr.getDelegated().type() instanceof OpenSearchDateType
              && List.of(TIMESTAMP, TIME, DATE)
                  .contains(((OpenSearchDateType) expr.getDelegated().type()).getExprCoreType()))
          || List.of(TIMESTAMP, TIME, DATE).contains(expr.getDelegated().type())) {
        sourceBuilder.userValueTypeHint(ValueType.LONG);
      }
      return helper.build(expr.getDelegated(), sourceBuilder::field, sourceBuilder::script);
    }
  }

  /** Build the MultiTermsAggregationBuilder. */
  public MultiTermsAggregationBuilder buildMultipleTerms(List<NamedExpression> exprs) {
    MultiTermsAggregationBuilder sourceBuilder =
        new MultiTermsAggregationBuilder(
            exprs.stream().map(NamedExpression::getName).collect(Collectors.joining("_")));
    sourceBuilder.terms(
        exprs.stream()
            .map(
                expr -> {
                  MultiTermsValuesSourceConfig.Builder config =
                      new MultiTermsValuesSourceConfig.Builder();
                  config.setFieldName(expr.getName());
                  // Time types values are converted to LONG in ExpressionAggregationScript::execute
                  if ((expr.getDelegated().type() instanceof OpenSearchDateType
                          && List.of(TIMESTAMP, TIME, DATE)
                              .contains(
                                  ((OpenSearchDateType) expr.getDelegated().type())
                                      .getExprCoreType()))
                      || List.of(TIMESTAMP, TIME, DATE).contains(expr.getDelegated().type())) {
                    config.setUserValueTypeHint(ValueType.LONG);
                  }
                  return config.build();
                })
            .toList());
    sourceBuilder.size(AGGREGATION_BUCKET_SIZE);
    return sourceBuilder;
  }

  public static ValuesSourceAggregationBuilder<?> buildHistogram(
      String name, String field, Double value, SpanUnit unit) {
    switch (unit) {
      case NONE:
        return new HistogramAggregationBuilder(name).field(field).interval(value);
      case UNKNOWN:
        throw new IllegalStateException("Invalid span unit");
      default:
        return buildDateHistogram(name, field, value.intValue(), unit);
    }
  }

  public static ValuesSourceAggregationBuilder<?> buildAutoDateHistogram(
      String name, String field, Integer bucketSize) {
    return new AutoDateHistogramAggregationBuilder(name).field(field).setNumBuckets(bucketSize);
  }

  public static ValuesSourceAggregationBuilder<?> buildDateHistogram(
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

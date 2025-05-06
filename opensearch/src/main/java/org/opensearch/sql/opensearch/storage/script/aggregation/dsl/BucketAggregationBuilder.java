/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.commons.lang3.tuple.Triple;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

/** Bucket Aggregation Builder. */
public class BucketAggregationBuilder {

  private final AggregationBuilderHelper helper;

  public BucketAggregationBuilder(ExpressionSerializer serializer) {
    this.helper = new AggregationBuilderHelper(serializer);
  }

  /** Build the list of CompositeValuesSourceBuilder. */
  public List<CompositeValuesSourceBuilder<?>> build(
      List<Triple<NamedExpression, SortOrder, MissingOrder>> groupList) {
    ImmutableList.Builder<CompositeValuesSourceBuilder<?>> resultBuilder =
        new ImmutableList.Builder<>();
    for (Triple<NamedExpression, SortOrder, MissingOrder> groupPair : groupList) {
      resultBuilder.add(
          buildCompositeValuesSourceBuilder(
              groupPair.getLeft(), groupPair.getMiddle(), groupPair.getRight()));
    }
    return resultBuilder.build();
  }

  // todo, Expression should implement buildCompositeValuesSourceBuilder() interface.
  private CompositeValuesSourceBuilder<?> buildCompositeValuesSourceBuilder(
      NamedExpression expr, SortOrder sortOrder, MissingOrder missingOrder) {
    if (expr.getDelegated() instanceof SpanExpression) {
      SpanExpression spanExpr = (SpanExpression) expr.getDelegated();
      return buildHistogram(
          expr.getName(),
          spanExpr.getField().toString(),
          spanExpr.getValue().valueOf().doubleValue(),
          spanExpr.getUnit(),
          missingOrder);
    } else {
      CompositeValuesSourceBuilder<?> sourceBuilder =
          new TermsValuesSourceBuilder(expr.getName())
              .missingBucket(true)
              .missingOrder(missingOrder)
              .order(sortOrder);
      // Time types values are converted to LONG in ExpressionAggregationScript::execute
      if ((expr.getDelegated().type() instanceof OpenSearchDateType
              && List.of(TIMESTAMP, TIME, DATE)
                  .contains(((OpenSearchDateType) expr.getDelegated().type()).getExprCoreType()))
          || List.of(TIMESTAMP, TIME, DATE).contains(expr.getDelegated().type())) {
        sourceBuilder.userValuetypeHint(ValueType.LONG);
      }
      return helper.build(expr.getDelegated(), sourceBuilder::field, sourceBuilder::script);
    }
  }

  private CompositeValuesSourceBuilder<?> buildHistogram(
      String name, String field, Double value, SpanUnit unit, MissingOrder missingOrder) {
    switch (unit) {
      case NONE:
        return new HistogramValuesSourceBuilder(name)
            .field(field)
            .interval(value)
            .missingBucket(true)
            .missingOrder(missingOrder);
      case UNKNOWN:
        throw new IllegalStateException("Invalid span unit");
      default:
        return buildDateHistogram(name, field, value.intValue(), unit, missingOrder);
    }
  }

  private CompositeValuesSourceBuilder<?> buildDateHistogram(
      String name, String field, Integer value, SpanUnit unit, MissingOrder missingOrder) {
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
        return new DateHistogramValuesSourceBuilder(name)
            .field(field)
            .missingBucket(true)
            .missingOrder(missingOrder)
            .fixedInterval(new DateHistogramInterval(spanValue));
      default:
        return new DateHistogramValuesSourceBuilder(name)
            .field(field)
            .missingBucket(true)
            .missingOrder(missingOrder)
            .calendarInterval(new DateHistogramInterval(spanValue));
    }
  }
}

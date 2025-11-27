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
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.storage.serde.ExpressionSerializer;

/** Composite Aggregation Builder. */
public class CompositeAggregationBuilder {

  private final AggregationBuilderHelper helper;

  public CompositeAggregationBuilder(ExpressionSerializer serializer) {
    this.helper = new AggregationBuilderHelper(serializer);
  }

  /** Build the list of CompositeValuesSourceBuilder. */
  public List<CompositeValuesSourceBuilder<?>> build(
      List<Triple<NamedExpression, SortOrder, MissingOrder>> groupList, boolean bucketNullable) {
    ImmutableList.Builder<CompositeValuesSourceBuilder<?>> resultBuilder =
        new ImmutableList.Builder<>();
    for (Triple<NamedExpression, SortOrder, MissingOrder> groupPair : groupList) {
      resultBuilder.add(
          buildCompositeValuesSourceBuilder(
              groupPair.getLeft(), groupPair.getMiddle(), groupPair.getRight(), bucketNullable));
    }
    return resultBuilder.build();
  }

  // todo, Expression should implement buildCompositeValuesSourceBuilder() interface.
  private CompositeValuesSourceBuilder<?> buildCompositeValuesSourceBuilder(
      NamedExpression expr,
      SortOrder sortOrder,
      MissingOrder missingOrder,
      boolean bucketNullable) {
    if (expr.getDelegated() instanceof SpanExpression) {
      SpanExpression spanExpr = (SpanExpression) expr.getDelegated();
      return buildHistogram(
          expr.getName(),
          spanExpr.getField().toString(),
          spanExpr.getValue().valueOf().doubleValue(),
          spanExpr.getUnit(),
          missingOrder,
          bucketNullable);
    } else {
      CompositeValuesSourceBuilder<?> sourceBuilder =
          new TermsValuesSourceBuilder(expr.getName()).order(sortOrder);
      if (bucketNullable) {
        sourceBuilder.missingBucket(true).missingOrder(missingOrder);
      }
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

  public static CompositeValuesSourceBuilder<?> buildHistogram(
      String name,
      String field,
      Double value,
      SpanUnit unit,
      MissingOrder missingOrder,
      boolean bucketNullable) {
    switch (unit) {
      case NONE:
        HistogramValuesSourceBuilder histogramBuilder =
            new HistogramValuesSourceBuilder(name).field(field).interval(value);
        if (bucketNullable) {
          histogramBuilder.missingBucket(true).missingOrder(missingOrder);
        }
        return histogramBuilder;
      case UNKNOWN:
        throw new IllegalStateException("Invalid span unit");
      default:
        return buildDateHistogram(name, field, value.intValue(), unit);
    }
  }

  public static CompositeValuesSourceBuilder<?> buildDateHistogram(
      String name, String field, Integer value, SpanUnit unit) {
    String spanValue = value + unit.getName();
    DateHistogramValuesSourceBuilder builder =
        new DateHistogramValuesSourceBuilder(name).field(field);
    return useCalendarInterval(spanValue)
        ? builder.calendarInterval(new DateHistogramInterval(spanValue))
        : builder.fixedInterval(new DateHistogramInterval(spanValue));
  }

  private static boolean useCalendarInterval(String spanValue) {
    return DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(spanValue);
  }
}

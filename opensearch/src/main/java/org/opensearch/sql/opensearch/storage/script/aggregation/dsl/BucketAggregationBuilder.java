/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

/**
 * Bucket Aggregation Builder.
 */
public class BucketAggregationBuilder {

  private final AggregationBuilderHelper helper;

  public BucketAggregationBuilder(
      ExpressionSerializer serializer) {
    this.helper = new AggregationBuilderHelper(serializer);
  }

  /**
   * Build the list of CompositeValuesSourceBuilder.
   */
  public List<CompositeValuesSourceBuilder<?>> build(
      List<Pair<NamedExpression, SortOrder>> groupList) {
    ImmutableList.Builder<CompositeValuesSourceBuilder<?>> resultBuilder =
        new ImmutableList.Builder<>();
    for (Pair<NamedExpression, SortOrder> groupPair : groupList) {
      TermsValuesSourceBuilder valuesSourceBuilder =
          new TermsValuesSourceBuilder(groupPair.getLeft().getNameOrAlias())
              .missingBucket(true)
              .order(groupPair.getRight());
      resultBuilder
          .add((CompositeValuesSourceBuilder<?>) helper.build(groupPair.getLeft().getDelegated(),
              valuesSourceBuilder::field, valuesSourceBuilder::script));
    }
    return resultBuilder.build();
  }
}

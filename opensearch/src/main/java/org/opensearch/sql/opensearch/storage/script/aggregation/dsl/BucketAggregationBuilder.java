/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Spark project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.sql.opensearch.request;

import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_INDEX;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.NamedFieldExpression;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.MetricParser;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.response.agg.StatsParser;

/**
 * Aggregate analyzer. Convert aggregate to AggregationBuilder {@link AggregationBuilder} and its
 * related Parser {@link OpenSearchAggregationResponseParser}.
 */
public class AggregateAnalyzer {

  /** How many composite buckets should be returned. */
  public static final int AGGREGATION_BUCKET_SIZE = 1000;

  /** metadata field used when there is no argument. Only apply to COUNT. */
  private static final String METADATA_FIELD = "_index";

  /** Internal exception. */
  @SuppressWarnings("serial")
  public static final class AggregateAnalyzerException extends RuntimeException {

    AggregateAnalyzerException(String message) {
      super(message);
    }

    AggregateAnalyzerException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Exception that is thrown when a {@link Aggregate} cannot be processed (or converted into an
   * OpenSearch aggregate query).
   */
  public static class ExpressionNotAnalyzableException extends Exception {
    ExpressionNotAnalyzableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private AggregateAnalyzer() {}

  // TODO: should we support filter aggregation? For PPL, we don't have filter in stats command
  // TODO: support script pushdown for aggregation. Calcite doesn't expression in its AggregateCall
  // or GroupSet
  // https://github.com/opensearch-project/sql/issues/3386
  //
  public static Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> analyze(
      Aggregate aggregate,
      List<String> schema,
      Map<String, ExprType> fieldTypes,
      List<String> outputFields)
      throws ExpressionNotAnalyzableException {
    requireNonNull(aggregate, "aggregate");
    try {
      List<Integer> groupList = aggregate.getGroupSet().asList();
      FieldExpressionCreator fieldExpressionCreator =
          fieldIndex -> new NamedFieldExpression(fieldIndex, schema, fieldTypes);
      // Process all aggregate calls
      Pair<Builder, List<MetricParser>> builderAndParser =
          processAggregateCalls(
              groupList.size(), aggregate.getAggCallList(), fieldExpressionCreator, outputFields);
      Builder metricBuilder = builderAndParser.getLeft();
      List<MetricParser> metricParserList = builderAndParser.getRight();

      if (aggregate.getGroupSet().isEmpty()) {
        return Pair.of(
            ImmutableList.copyOf(metricBuilder.getAggregatorFactories()),
            new NoBucketAggregationParser(metricParserList));
      } else {
        List<CompositeValuesSourceBuilder<?>> buckets =
            createCompositeBuckets(groupList, fieldExpressionCreator);
        return Pair.of(
            Collections.singletonList(
                AggregationBuilders.composite("composite_buckets", buckets)
                    .subAggregations(metricBuilder)
                    .size(AGGREGATION_BUCKET_SIZE)),
            new CompositeAggregationParser(metricParserList));
      }
    } catch (Throwable e) {
      Throwables.throwIfInstanceOf(e, UnsupportedOperationException.class);
      throw new ExpressionNotAnalyzableException("Can't convert " + aggregate, e);
    }
  }

  private static Pair<Builder, List<MetricParser>> processAggregateCalls(
      int groupOffset,
      List<AggregateCall> aggCalls,
      FieldExpressionCreator fieldExpressionCreator,
      List<String> outputFields) {
    assert aggCalls.size() + groupOffset == outputFields.size()
        : "groups size and agg calls size should match with output fields";
    Builder metricBuilder = new AggregatorFactories.Builder();
    List<MetricParser> metricParserList = new ArrayList<>();

    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      String argStr =
          aggCall.getAggregation().kind == SqlKind.COUNT && aggCall.getArgList().isEmpty()
              ? METADATA_FIELD_INDEX
              : fieldExpressionCreator
                  .create(aggCall.getArgList().get(0))
                  .getReferenceForTermQuery();
      String aggField = outputFields.get(groupOffset + i);

      Pair<ValuesSourceAggregationBuilder<?>, MetricParser> builderAndParser =
          createAggregationBuilderAndParser(aggCall, argStr, aggField);
      metricBuilder.addAggregator(builderAndParser.getLeft());
      metricParserList.add(builderAndParser.getRight());
    }
    return Pair.of(metricBuilder, metricParserList);
  }

  private interface FieldExpressionCreator {
    NamedFieldExpression create(int fieldIndex);
  }

  private static Pair<ValuesSourceAggregationBuilder<?>, MetricParser>
      createAggregationBuilderAndParser(AggregateCall aggCall, String argStr, String aggField) {
    if (aggCall.isDistinct()) {
      return createDistinctAggregation(aggCall, argStr, aggField);
    } else {
      return createRegularAggregation(aggCall, argStr, aggField);
    }
  }

  private static Pair<ValuesSourceAggregationBuilder<?>, MetricParser> createDistinctAggregation(
      AggregateCall aggCall, String argStr, String aggField) {
    switch (aggCall.getAggregation().kind) {
      case COUNT:
        return Pair.of(
                AggregationBuilders.cardinality(aggField).field(argStr),
                new SingleValueParser(aggField));
      default:
        throw new AggregateAnalyzer.AggregateAnalyzerException(
                String.format("unsupported distinct aggregator %s", aggCall.getAggregation()));
    }
  }

  private static Pair<ValuesSourceAggregationBuilder<?>, MetricParser> createRegularAggregation(
      AggregateCall aggCall, String argStr, String aggField) {

    switch (aggCall.getAggregation().kind) {
      case AVG:
        return Pair.of(
                AggregationBuilders.avg(aggField).field(argStr),
                new SingleValueParser(aggField));
      case SUM:
        return Pair.of(
                AggregationBuilders.sum(aggField).field(argStr),
                new SingleValueParser(aggField));
      case COUNT:
        return Pair.of(
                AggregationBuilders.count(aggField).field(argStr),
                new SingleValueParser(aggField));
      case MIN:
        return Pair.of(
                AggregationBuilders.min(aggField).field(argStr),
                new SingleValueParser(aggField));
      case MAX:
        return Pair.of(
                AggregationBuilders.max(aggField).field(argStr),
                new SingleValueParser(aggField));
      case VAR_SAMP:
        return Pair.of(
                AggregationBuilders.extendedStats(aggField).field(argStr),
                new StatsParser(ExtendedStats::getVarianceSampling, aggField));
      case VAR_POP:
        return Pair.of(
                AggregationBuilders.extendedStats(aggField).field(argStr),
                new StatsParser(ExtendedStats::getVariancePopulation, aggField));
      case STDDEV_SAMP:
        return Pair.of(
                AggregationBuilders.extendedStats(aggField).field(argStr),
                new StatsParser(ExtendedStats::getStdDeviationSampling, aggField));
      case STDDEV_POP:
        return Pair.of(
                AggregationBuilders.extendedStats(aggField).field(argStr),
                new StatsParser(ExtendedStats::getStdDeviationPopulation, aggField));
      default:
        throw new AggregateAnalyzerException(
                String.format("unsupported aggregator %s", aggCall.getAggregation()));
    }

  }

  private static List<CompositeValuesSourceBuilder<?>> createCompositeBuckets(
      List<Integer> groupList, FieldExpressionCreator fieldExpressionCreator) {

    ImmutableList.Builder<CompositeValuesSourceBuilder<?>> resultBuilder = ImmutableList.builder();

    for (int groupIndex : groupList) {
      NamedFieldExpression groupExpr = fieldExpressionCreator.create(groupIndex);

      // TODO: support histogram bucket(i.e. PPL span expression)
      // https://github.com/opensearch-project/sql/issues/3384
      CompositeValuesSourceBuilder<?> sourceBuilder = createTermsSourceBuilder(groupExpr);

      resultBuilder.add(sourceBuilder);
    }

    return resultBuilder.build();
  }

  private static CompositeValuesSourceBuilder<?> createTermsSourceBuilder(
      NamedFieldExpression groupExpr) {

    CompositeValuesSourceBuilder<?> sourceBuilder =
        new TermsValuesSourceBuilder(groupExpr.getRootName())
            .missingBucket(true)
            // TODO: use Sort's option if there is Sort push-down into aggregation
            // https://github.com/opensearch-project/sql/issues/3380
            .missingOrder(MissingOrder.FIRST)
            .order(SortOrder.ASC)
            .field(groupExpr.getReferenceForTermQuery());

    // Time types values are converted to LONG in ExpressionAggregationScript::execute
    if (List.of(TIMESTAMP, TIME, DATE).contains(groupExpr.getExprType())) {
      sourceBuilder.userValuetypeHint(ValueType.LONG);
    }

    return sourceBuilder;
  }
}

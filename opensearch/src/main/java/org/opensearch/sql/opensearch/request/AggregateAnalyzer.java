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
import static org.opensearch.sql.expression.function.PPLBuiltinOperators.WIDTH_BUCKET;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.NamedFieldExpression;
import org.opensearch.sql.opensearch.response.agg.ArgMaxMinParser;
import org.opensearch.sql.opensearch.response.agg.BucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.CountAsTotalHitsParser;
import org.opensearch.sql.opensearch.response.agg.MetricParser;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SinglePercentileParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.response.agg.StatsParser;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;
import org.opensearch.sql.opensearch.storage.script.aggregation.dsl.BucketAggregationBuilder;
import org.opensearch.sql.opensearch.storage.script.aggregation.dsl.CompositeAggregationBuilder;

/**
 * Aggregate analyzer. Convert aggregate to AggregationBuilder {@link AggregationBuilder} and its
 * related Parser {@link OpenSearchAggregationResponseParser}.
 */
public class AggregateAnalyzer {

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

  public static class CompositeAggUnSupportedException extends RuntimeException {
    CompositeAggUnSupportedException(String message) {
      super(message);
    }
  }

  private AggregateAnalyzer() {}

  @RequiredArgsConstructor
  static class AggregateBuilderHelper {
    final RelDataType rowType;
    final Map<String, ExprType> fieldTypes;
    final RelOptCluster cluster;
    final boolean bucketNullable;
    final int bucketSize;

    <T extends ValuesSourceAggregationBuilder<T>> T build(RexNode node, T aggBuilder) {
      return build(node, aggBuilder::field, aggBuilder::script);
    }

    <T extends CompositeValuesSourceBuilder<T>> T build(RexNode node, T sourceBuilder) {
      return build(node, sourceBuilder::field, sourceBuilder::script);
    }

    <T> T build(RexNode node, Function<String, T> fieldBuilder, Function<Script, T> scriptBuilder) {
      if (node == null) return fieldBuilder.apply(METADATA_FIELD);
      else if (node instanceof RexInputRef) {
        RexInputRef ref = (RexInputRef) node;
        return fieldBuilder.apply(
            new NamedFieldExpression(ref.getIndex(), rowType.getFieldNames(), fieldTypes)
                .getReferenceForTermQuery());
      } else if (node instanceof RexCall || node instanceof RexLiteral) {
        return scriptBuilder.apply(
            (new PredicateAnalyzer.ScriptQueryExpression(node, rowType, fieldTypes, cluster))
                .getScript());
      }
      throw new IllegalStateException(
          String.format("Metric aggregation doesn't support RexNode %s", node));
    }

    NamedFieldExpression inferNamedField(RexNode node) {
      if (node instanceof RexInputRef) {
        RexInputRef ref = (RexInputRef) node;
        return new NamedFieldExpression(ref.getIndex(), rowType.getFieldNames(), fieldTypes);
      }
      throw new IllegalStateException(
          String.format("Cannot infer field name from RexNode %s", node));
    }

    <T> T inferValue(RexNode node, Class<T> clazz) {
      if (node instanceof RexLiteral) {
        RexLiteral literal = (RexLiteral) node;
        return literal.getValueAs(clazz);
      }
      throw new IllegalStateException(String.format("Cannot infer value from RexNode %s", node));
    }
  }

  // TODO: should we support filter aggregation? For PPL, we don't have filter in stats command
  public static Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> analyze(
      Aggregate aggregate,
      Project project,
      RelDataType rowType,
      Map<String, ExprType> fieldTypes,
      List<String> outputFields,
      RelOptCluster cluster,
      int bucketSize)
      throws ExpressionNotAnalyzableException {
    requireNonNull(aggregate, "aggregate");
    try {
      boolean bucketNullable =
          Boolean.parseBoolean(
              aggregate.getHints().stream()
                  .filter(hits -> hits.hintName.equals("stats_args"))
                  .map(hint -> hint.kvOptions.getOrDefault(Argument.BUCKET_NULLABLE, "true"))
                  .findFirst()
                  .orElseGet(() -> "true"));
      List<Integer> groupList = aggregate.getGroupSet().asList();
      AggregateBuilderHelper helper =
          new AggregateBuilderHelper(rowType, fieldTypes, cluster, bucketNullable, bucketSize);
      List<String> aggFieldNames = outputFields.subList(groupList.size(), outputFields.size());
      // Process all aggregate calls
      Pair<Builder, List<MetricParser>> builderAndParser =
          processAggregateCalls(aggFieldNames, aggregate.getAggCallList(), project, helper);
      Builder metricBuilder = builderAndParser.getLeft();
      List<MetricParser> metricParserList = builderAndParser.getRight();

      // both count() and count(FIELD) can apply doc_count optimization in non-bucket aggregation,
      // but only count() can apply doc_count optimization in bucket aggregation.
      boolean countAllOnly = !aggregate.getGroupSet().isEmpty();
      Pair<List<String>, Builder> countAggNameAndBuilderPair =
          removeCountAggregationBuilders(metricBuilder, countAllOnly);
      Builder newMetricBuilder = countAggNameAndBuilderPair.getRight();
      List<String> countAggNames = countAggNameAndBuilderPair.getLeft();

      if (aggregate.getGroupSet().isEmpty()) {
        if (newMetricBuilder == null) {
          // The optimization must require all count aggregations are removed,
          // and they have only one field name
          return Pair.of(List.of(), new CountAsTotalHitsParser(countAggNames));
        } else {
          return Pair.of(
              ImmutableList.copyOf(newMetricBuilder.getAggregatorFactories()),
              new NoBucketAggregationParser(metricParserList));
        }
      } else if (aggregate.getGroupSet().length() == 1
          && isAutoDateSpan(project.getProjects().get(groupList.get(0)))) {
        ValuesSourceAggregationBuilder<?> bucketBuilder = createBucket(0, project, helper);
        if (newMetricBuilder != null) {
          bucketBuilder.subAggregations(newMetricBuilder);
        }
        return Pair.of(
            Collections.singletonList(bucketBuilder),
            new BucketAggregationParser(metricParserList, countAggNames));
      } else {
        AggregationBuilder aggregationBuilder;
        try {
          List<CompositeValuesSourceBuilder<?>> buckets =
              createCompositeBuckets(groupList, project, helper);
          aggregationBuilder =
              AggregationBuilders.composite("composite_buckets", buckets).size(bucketSize);
          if (newMetricBuilder != null) {
            aggregationBuilder.subAggregations(metricBuilder);
          }
          return Pair.of(
              Collections.singletonList(aggregationBuilder),
              new CompositeAggregationParser(metricParserList, countAggNames));
        } catch (CompositeAggUnSupportedException e) {
          if (bucketNullable) {
            throw new UnsupportedOperationException(e.getMessage());
          }
          aggregationBuilder = createNestedBuckets(groupList, project, newMetricBuilder, helper);
          return Pair.of(
              Collections.singletonList(aggregationBuilder),
              new BucketAggregationParser(metricParserList, countAggNames));
        }
      }
    } catch (Throwable e) {
      Throwables.throwIfInstanceOf(e, UnsupportedOperationException.class);
      throw new ExpressionNotAnalyzableException("Can't convert " + aggregate, e);
    }
  }

  /**
   * Remove all ValueCountAggregationBuilder from metric builder, and return the name list for the
   * removed count aggs with the updated metric builder.
   *
   * @param metricBuilder metrics builder
   * @param countAllOnly remove count() only, or count(FIELD) will be removed.
   * @return a pair of name list for the removed count aggs and updated metric builder. If the count
   *     aggregations cannot satisfy the requirement to remove, it will return an empty name list
   *     with the original metric builder.
   */
  private static Pair<List<String>, Builder> removeCountAggregationBuilders(
      Builder metricBuilder, boolean countAllOnly) {
    List<ValueCountAggregationBuilder> countAggregatorFactories =
        metricBuilder.getAggregatorFactories().stream()
            .filter(ValueCountAggregationBuilder.class::isInstance)
            .map(ValueCountAggregationBuilder.class::cast)
            .filter(vc -> vc.script() == null)
            .filter(vc -> !countAllOnly || vc.fieldName().equals("_index"))
            .collect(Collectors.toList());
    List<AggregationBuilder> copy = new ArrayList<>(metricBuilder.getAggregatorFactories());
    copy.removeAll(countAggregatorFactories);
    Builder newMetricBuilder = new AggregatorFactories.Builder();
    copy.forEach(newMetricBuilder::addAggregator);

    if (countAllOnly || supportCountFiled(countAggregatorFactories, metricBuilder)) {
      List<String> countAggNameList =
          countAggregatorFactories.stream().map(ValuesSourceAggregationBuilder::getName).collect(
              Collectors.toList());
      if (newMetricBuilder.getAggregatorFactories().isEmpty()) {
        newMetricBuilder = null;
      }
      return Pair.of(countAggNameList, newMetricBuilder);
    }
    return Pair.of(List.of(), metricBuilder);
  }

  private static boolean supportCountFiled(
      List<ValueCountAggregationBuilder> countAggBuilderList, Builder metricBuilder) {
    return countAggBuilderList.size() == metricBuilder.getAggregatorFactories().size()
        && countAggBuilderList.stream()
                .map(ValuesSourceAggregationBuilder::fieldName)
                .distinct()
                .count()
            == 1;
  }

  private static Pair<Builder, List<MetricParser>> processAggregateCalls(
      List<String> aggFieldNames,
      List<AggregateCall> aggCalls,
      Project project,
      AggregateAnalyzer.AggregateBuilderHelper helper)
      throws PredicateAnalyzer.ExpressionNotAnalyzableException {
    Builder metricBuilder = new AggregatorFactories.Builder();
    List<MetricParser> metricParserList = new ArrayList<>();
    AggregateFilterAnalyzer aggFilterAnalyzer = new AggregateFilterAnalyzer(helper, project);

    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      List<RexNode> args = convertAggArgThroughProject(aggCall, project);
      String aggFieldName = aggFieldNames.get(i);

      Pair<AggregationBuilder, MetricParser> builderAndParser =
          createAggregationBuilderAndParser(aggCall, args, aggFieldName, helper);
      builderAndParser = aggFilterAnalyzer.analyze(builderAndParser, aggCall, aggFieldName);
      metricBuilder.addAggregator(builderAndParser.getLeft());
      metricParserList.add(builderAndParser.getRight());
    }
    return Pair.of(metricBuilder, metricParserList);
  }

  private static List<RexNode> convertAggArgThroughProject(AggregateCall aggCall, Project project) {
    return project == null
        ? List.of()
        : aggCall.getArgList().stream().map(project.getProjects()::get).collect(Collectors.toList());
  }

  private static Pair<AggregationBuilder, MetricParser> createAggregationBuilderAndParser(
      AggregateCall aggCall,
      List<RexNode> args,
      String aggFieldName,
      AggregateAnalyzer.AggregateBuilderHelper helper) {
    if (aggCall.isDistinct()) {
      return createDistinctAggregation(aggCall, args, aggFieldName, helper);
    } else {
      return createRegularAggregation(aggCall, args, aggFieldName, helper);
    }
  }

  private static Pair<AggregationBuilder, MetricParser> createDistinctAggregation(
      AggregateCall aggCall,
      List<RexNode> args,
      String aggFieldName,
      AggregateBuilderHelper helper) {
    switch (aggCall.getAggregation().kind) {
      case COUNT:
        return Pair.of(
                helper.build(
              !args.isEmpty() ? args.get(0) : null,
              AggregationBuilders.cardinality(aggFieldName)),
                new SingleValueParser(aggFieldName));
      default:
        throw new AggregateAnalyzer.AggregateAnalyzerException(
                String.format("unsupported distinct aggregator %s", aggCall.getAggregation()));
    }
  }

  private static Pair<AggregationBuilder, MetricParser> createRegularAggregation(
      AggregateCall aggCall,
      List<RexNode> args,
      String aggFieldName,
      AggregateBuilderHelper helper) {

    switch (aggCall.getAggregation().kind) {
      case AVG:
        return Pair.of(
            helper.build(args.get(0), AggregationBuilders.avg(aggFieldName)),
            new SingleValueParser(aggFieldName));
      case SUM:
        // 1. Only case SUM, skip SUM0 / COUNT since calling avg() in DSL should be faster.
        // 2. To align with databases, SUM0 is not preferred now.
        return Pair.of(
            helper.build(args.get(0), AggregationBuilders.sum(aggFieldName)),
            new SingleValueParser(aggFieldName));
      case COUNT:
        return Pair.of(
            helper.build(
                !args.isEmpty() ? args.get(0) : null, AggregationBuilders.count(aggFieldName)),
            new SingleValueParser(aggFieldName));
      case MIN: {
        ExprType fieldType =
            OpenSearchTypeFactory.convertRelDataTypeToExprType(args.get(0).getType());
        if (supportsMaxMinAggregation(fieldType)) {
          return Pair.of(
              helper.build(args.get(0), AggregationBuilders.min(aggFieldName)),
              new SingleValueParser(aggFieldName));
        } else {
          return Pair.of(
              AggregationBuilders.topHits(aggFieldName)
                  .fetchSource(helper.inferNamedField(args.get(0)).getRootName(), null)
                  .size(1)
                  .from(0)
                  .sort(
                      helper.inferNamedField(args.get(0)).getReferenceForTermQuery(),
                      SortOrder.ASC),
              new TopHitsParser(aggFieldName, true));
        }
      }
      case MAX: {
        ExprType fieldType =
            OpenSearchTypeFactory.convertRelDataTypeToExprType(args.get(0).getType());
        if (supportsMaxMinAggregation(fieldType)) {
          return Pair.of(
              helper.build(args.get(0), AggregationBuilders.max(aggFieldName)),
              new SingleValueParser(aggFieldName));
        } else {
          return Pair.of(
              AggregationBuilders.topHits(aggFieldName)
                  .fetchSource(helper.inferNamedField(args.get(0)).getRootName(), null)
                  .size(1)
                  .from(0)
                  .sort(
                      helper.inferNamedField(args.get(0)).getReferenceForTermQuery(),
                      SortOrder.DESC),
              new TopHitsParser(aggFieldName, true));
        }
      }
      case VAR_SAMP:
        return Pair.of(
            helper.build(args.get(0), AggregationBuilders.extendedStats(aggFieldName)),
            new StatsParser(ExtendedStats::getVarianceSampling, aggFieldName));
      case VAR_POP:
        return Pair.of(
            helper.build(args.get(0), AggregationBuilders.extendedStats(aggFieldName)),
            new StatsParser(ExtendedStats::getVariancePopulation, aggFieldName));
      case STDDEV_SAMP:
        return Pair.of(
            helper.build(args.get(0), AggregationBuilders.extendedStats(aggFieldName)),
            new StatsParser(ExtendedStats::getStdDeviationSampling, aggFieldName));
      case STDDEV_POP:
        return Pair.of(
            helper.build(args.get(0), AggregationBuilders.extendedStats(aggFieldName)),
            new StatsParser(ExtendedStats::getStdDeviationPopulation, aggFieldName));
      case ARG_MAX:
        return Pair.of(
          AggregationBuilders.topHits(aggFieldName)
              .fetchSource(helper.inferNamedField(args.get(0)).getRootName(), null)
              .size(1)
              .from(0)
              .sort(
                  helper.inferNamedField(args.get(1)).getRootName(),
                  org.opensearch.search.sort.SortOrder.DESC),
          new ArgMaxMinParser(aggFieldName));
      case ARG_MIN:
        return Pair.of(
          AggregationBuilders.topHits(aggFieldName)
              .fetchSource(helper.inferNamedField(args.get(0)).getRootName(), null)
              .size(1)
              .from(0)
              .sort(
                  helper.inferNamedField(args.get(1)).getRootName(),
                  org.opensearch.search.sort.SortOrder.ASC),
          new ArgMaxMinParser(aggFieldName));
      case OTHER_FUNCTION:
        BuiltinFunctionName functionName =
            BuiltinFunctionName.ofAggregation(aggCall.getAggregation().getName()).get();
        switch (functionName) {
          case TAKE:
            return Pair.of(
                AggregationBuilders.topHits(aggFieldName)
                    .fetchSource(helper.inferNamedField(args.get(0)).getRootName(), null)
                    .size(helper.inferValue(args.get(1), Integer.class))
                    .from(0),
                new TopHitsParser(aggFieldName));
          case FIRST:
            TopHitsAggregationBuilder firstBuilder =
                AggregationBuilders.topHits(aggFieldName).size(1).from(0);
            if (!args.isEmpty()) {
              firstBuilder.fetchSource(helper.inferNamedField(args.get(0)).getRootName(), null);
            }
            return Pair.of(firstBuilder, new TopHitsParser(aggFieldName, true));
          case LAST:
            TopHitsAggregationBuilder lastBuilder =
                AggregationBuilders.topHits(aggFieldName)
                    .size(1)
                    .from(0)
                    .sort("_doc", org.opensearch.search.sort.SortOrder.DESC);
            if (!args.isEmpty()) {
              lastBuilder.fetchSource(helper.inferNamedField(args.get(0)).getRootName(), null);
            }
            return Pair.of(lastBuilder, new TopHitsParser(aggFieldName, true));
          case PERCENTILE_APPROX:
            PercentilesAggregationBuilder aggBuilder =
                helper
                    .build(args.get(0), AggregationBuilders.percentiles(aggFieldName))
                    .percentiles(helper.inferValue(args.get(1), Double.class));
            /* See {@link PercentileApproxFunction}, PERCENTILE_APPROX accepts args of [FIELD, PERCENTILE, TYPE, COMPRESSION(optional)] */
            if (args.size() > 3) {
              aggBuilder.compression(helper.inferValue(args.get(3), Double.class));
            }
            return Pair.of(aggBuilder, new SinglePercentileParser(aggFieldName));
          default:
            throw new AggregateAnalyzer.AggregateAnalyzerException(
                String.format("Unsupported push-down aggregator %s", aggCall.getAggregation()));
        }
      default:
        throw new AggregateAnalyzer.AggregateAnalyzerException(
            String.format("unsupported aggregator %s", aggCall.getAggregation()));
    }
  }

  private static boolean supportsMaxMinAggregation(ExprType fieldType) {
    ExprType coreType =
        (fieldType instanceof OpenSearchDataType)
            ? ((OpenSearchDataType) fieldType).getExprType()
            : fieldType;

    return ExprCoreType.numberTypes().contains(coreType)
        || coreType == ExprCoreType.DATE
        || coreType == ExprCoreType.TIME
        || coreType == ExprCoreType.TIMESTAMP;
  }

  private static List<CompositeValuesSourceBuilder<?>> createCompositeBuckets(
      List<Integer> groupList, Project project, AggregateAnalyzer.AggregateBuilderHelper helper) {
    ImmutableList.Builder<CompositeValuesSourceBuilder<?>> resultBuilder = ImmutableList.builder();
    groupList.forEach(
        groupIndex -> resultBuilder.add(createCompositeBucket(groupIndex, project, helper)));
    return resultBuilder.build();
  }

  private static ValuesSourceAggregationBuilder<?> createNestedBuckets(
      List<Integer> groupList,
      Project project,
      Builder metricBuilder,
      AggregateAnalyzer.AggregateBuilderHelper helper) {
    ValuesSourceAggregationBuilder<?> rootAgg = createBucket(groupList.get(0), project, helper);
    ValuesSourceAggregationBuilder<?> currentAgg = rootAgg;
    for (int i = 1; i < groupList.size(); i++) {
      ValuesSourceAggregationBuilder<?> nextAgg = createBucket(groupList.get(i), project, helper);
      currentAgg.subAggregations(new AggregatorFactories.Builder().addAggregator(nextAgg));
      currentAgg = nextAgg;
    }
    if (metricBuilder != null) {
      currentAgg.subAggregations(metricBuilder);
    }
    return rootAgg;
  }

  private static boolean isAutoDateSpan(RexNode rex) {
    return rex instanceof RexCall
        && ((RexCall)rex).getKind() == SqlKind.OTHER_FUNCTION
        && ((RexCall)rex).getOperator().equals(WIDTH_BUCKET);
  }

  private static ValuesSourceAggregationBuilder<?> createBucket(
      Integer groupIndex, Project project, AggregateBuilderHelper helper) {
    RexNode rex = project.getProjects().get(groupIndex);
    String bucketName = project.getRowType().getFieldList().get(groupIndex).getName();
    if (rex instanceof RexCall
        && rex.getKind() == SqlKind.OTHER_FUNCTION
        && ((RexCall) rex).getOperator().getName().equalsIgnoreCase(BuiltinFunctionName.SPAN.name())
        && ((RexCall) rex).getOperands().size() == 3
        && ((RexCall) rex).getOperands().get(0) instanceof RexInputRef
        && ((RexCall) rex).getOperands().get(1) instanceof RexLiteral
        && ((RexCall) rex).getOperands().get(2) instanceof RexLiteral) {
      return BucketAggregationBuilder.buildHistogram(
          bucketName,
          helper.inferNamedField(((RexCall) rex).getOperands().get(0)).getRootName(),
          ((RexLiteral)((RexCall) rex).getOperands().get(1)).getValueAs(Double.class),
          SpanUnit.of(((RexLiteral)((RexCall) rex).getOperands().get(2)).getValueAs(String.class)));
    } else if (isAutoDateSpan(rex)) {
      RexCall rexCall = (RexCall) rex;
      RexInputRef rexInputRef = (RexInputRef) rexCall.getOperands().get(0);
      RexLiteral valueLiteral = (RexLiteral) rexCall.getOperands().get(1);
      return new AutoDateHistogramAggregationBuilder(bucketName)
          .field(helper.inferNamedField(rexInputRef).getRootName())
          .setNumBuckets(requireNonNull(valueLiteral.getValueAs(Integer.class)));
    } else {
      return createTermsAggregationBuilder(bucketName, rex, helper);
    }
  }

  private static CompositeValuesSourceBuilder<?> createCompositeBucket(
      Integer groupIndex, Project project, AggregateAnalyzer.AggregateBuilderHelper helper)
      throws CompositeAggUnSupportedException {
    RexNode rex = project.getProjects().get(groupIndex);
    String bucketName = project.getRowType().getFieldList().get(groupIndex).getName();
    if (rex instanceof RexCall
        && rex.getKind() == SqlKind.OTHER_FUNCTION
        && ((RexCall) rex).getOperator().getName().equalsIgnoreCase(BuiltinFunctionName.SPAN.name())
        && ((RexCall) rex).getOperands().size() == 3
        && ((RexCall) rex).getOperands().get(0) instanceof RexInputRef
        && ((RexCall) rex).getOperands().get(1) instanceof RexLiteral
        && ((RexCall) rex).getOperands().get(2) instanceof RexLiteral) {
      return CompositeAggregationBuilder.buildHistogram(
          bucketName,
          helper.inferNamedField(((RexCall) rex).getOperands().get(0)).getRootName(),
          ((RexLiteral)((RexCall) rex).getOperands().get(1)).getValueAs(Double.class),
          SpanUnit.of(((RexLiteral)((RexCall) rex).getOperands().get(2)).getValueAs(String.class)),
          MissingOrder.FIRST,
          helper.bucketNullable);
    } else if (isAutoDateSpan(rex)) {
      throw new CompositeAggUnSupportedException(
          "auto_date_histogram is not supported in composite agg.");
    } else {
      return createTermsSourceBuilder(bucketName, rex, helper);
    }
  }

  private static CompositeValuesSourceBuilder<?> createTermsSourceBuilder(
      String bucketName, RexNode group, AggregateBuilderHelper helper) {
    TermsValuesSourceBuilder termsBuilder =
        new TermsValuesSourceBuilder(bucketName).order(SortOrder.ASC);
    if (helper.bucketNullable) {
      termsBuilder.missingBucket(true).missingOrder(MissingOrder.FIRST);
    }
    CompositeValuesSourceBuilder<?> sourceBuilder = helper.build(group, termsBuilder);

    return withValueTypeHint(
        sourceBuilder,
        sourceBuilder::userValuetypeHint,
        group.getType(),
        group instanceof RexInputRef);
  }

  private static ValuesSourceAggregationBuilder<?> createTermsAggregationBuilder(
      String bucketName, RexNode group, AggregateBuilderHelper helper) {
    TermsAggregationBuilder sourceBuilder =
        helper.build(
            group,
            new TermsAggregationBuilder(bucketName)
                .size(helper.bucketSize)
                .order(BucketOrder.key(true)));
    return withValueTypeHint(
        sourceBuilder,
        sourceBuilder::userValueTypeHint,
        group.getType(),
        group instanceof RexInputRef);
  }

  private static <T> T withValueTypeHint(
      T sourceBuilder,
      Function<ValueType, T> withValueTypeHint,
      RelDataType groupType,
      boolean isSourceField) {
    ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(groupType);
    // Time types values are converted to LONG in ExpressionAggregationScript::execute
    if (List.of(TIMESTAMP, TIME, DATE).contains(exprType)) {
      return withValueTypeHint.apply(ValueType.LONG);
    }
    // No need to set type hints for source fields
    if (isSourceField) {
      return sourceBuilder;
    }
    ValueType valueType = ValueType.lenientParse(exprType.typeName().toLowerCase());
    // The default value type is STRING, don't set that explicitly to avoid plan change.
    return valueType == null || valueType == ValueType.STRING
        ? sourceBuilder
        : withValueTypeHint.apply(valueType);
  }
}

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

  @RequiredArgsConstructor
  static class AggregateBuilderHelper {
    final RelDataType rowType;
    final Map<String, ExprType> fieldTypes;
    final RelOptCluster cluster;
    final boolean bucketNullable;

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
      RelOptCluster cluster)
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
          new AggregateBuilderHelper(rowType, fieldTypes, cluster, bucketNullable);
      List<String> aggFieldNames = outputFields.subList(groupList.size(), outputFields.size());
      // Process all aggregate calls
      Pair<Builder, List<MetricParser>> builderAndParser =
          processAggregateCalls(aggFieldNames, aggregate.getAggCallList(), project, helper);
      Builder metricBuilder = builderAndParser.getLeft();
      List<MetricParser> metricParserList = builderAndParser.getRight();

      // both count() and count(FIELD) can apply doc_count optimization in non-bucket aggregation,
      // but only count() can apply doc_count optimization in bucket aggregation.
      boolean countAllOnly = !aggregate.getGroupSet().isEmpty();
      Pair<List<ValueCountAggregationBuilder>, Builder> pair =
          removeCountAggregationBuilders(metricBuilder, countAllOnly);
      List<ValueCountAggregationBuilder> removedCountAggBuilders = pair.getLeft();
      Builder newMetricBuilder = pair.getRight();

      boolean removedCountAggBuildersHaveSomeField =
          removedCountAggBuilders.stream()
                  .map(ValuesSourceAggregationBuilder::fieldName)
                  .distinct()
                  .count()
              == 1;
      boolean allCountAggRemoved =
          removedCountAggBuilders.size() == metricBuilder.getAggregatorFactories().size();
      if (aggregate.getGroupSet().isEmpty()) {
        if (allCountAggRemoved && removedCountAggBuildersHaveSomeField) {
          // The optimization must require all count aggregations are removed,
          // and they have only one field name
          List<String> countAggNameList =
              removedCountAggBuilders.stream()
                  .map(ValuesSourceAggregationBuilder::getName)
                  .collect(Collectors.toList());
          return Pair.of(
              ImmutableList.copyOf(newMetricBuilder.getAggregatorFactories()),
              new CountAsTotalHitsParser(countAggNameList));
        } else {
          return Pair.of(
              ImmutableList.copyOf(metricBuilder.getAggregatorFactories()),
              new NoBucketAggregationParser(metricParserList));
        }
      } else {
        List<CompositeValuesSourceBuilder<?>> buckets =
            createCompositeBuckets(groupList, project, helper);
        AggregationBuilder aggregationBuilder =
            AggregationBuilders.composite("composite_buckets", buckets)
                .size(AGGREGATION_BUCKET_SIZE);

        // For bucket aggregation, no count() aggregator or not all aggregators are count(),
        // fallback to original ValueCountAggregation.
        if (removedCountAggBuilders.isEmpty()
            || removedCountAggBuilders.size() != metricBuilder.getAggregatorFactories().size()) {
          aggregationBuilder.subAggregations(metricBuilder);
          return Pair.of(
              Collections.singletonList(aggregationBuilder),
              new CompositeAggregationParser(metricParserList));
        }
        // No need to register sub-factories if no aggregator factories left after removing all
        // ValueCountAggregationBuilder.
        if (!newMetricBuilder.getAggregatorFactories().isEmpty()) {
          aggregationBuilder.subAggregations(newMetricBuilder);
        }
        List<String> countAggNameList =
            removedCountAggBuilders.stream().map(ValuesSourceAggregationBuilder::getName).collect(Collectors.toList());
        return Pair.of(
            Collections.singletonList(aggregationBuilder),
            new CompositeAggregationParser(metricParserList, countAggNameList));
      }
    } catch (Throwable e) {
      Throwables.throwIfInstanceOf(e, UnsupportedOperationException.class);
      throw new ExpressionNotAnalyzableException("Can't convert " + aggregate, e);
    }
  }

  /**
   * Remove all ValueCountAggregationBuilder from metric builder, and return the removed
   * ValueCountAggregationBuilder list.
   *
   * @param metricBuilder metrics builder
   * @param countAllOnly remove count() only, or count(FIELD) will be removed.
   * @return a pair of removed ValueCountAggregationBuilder and updated metric builder
   */
  private static Pair<List<ValueCountAggregationBuilder>, Builder> removeCountAggregationBuilders(
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
    return Pair.of(countAggregatorFactories, newMetricBuilder);
  }

  private static Pair<Builder, List<MetricParser>> processAggregateCalls(
      List<String> aggFieldNames,
      List<AggregateCall> aggCalls,
      Project project,
      AggregateBuilderHelper helper)
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
      AggregateBuilderHelper helper) {
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
        return Pair.of(
            helper.build(args.get(0), AggregationBuilders.sum(aggFieldName)),
            new SingleValueParser(aggFieldName));
      case COUNT:
        return Pair.of(
            helper.build(
                !args.isEmpty() ? args.get(0) : null, AggregationBuilders.count(aggFieldName)),
            new SingleValueParser(aggFieldName));
      case MIN: {
        String fieldName = helper.inferNamedField(args.get(0)).getRootName();
        ExprType fieldType = helper.fieldTypes.get(fieldName);

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
        String fieldName = helper.inferNamedField(args.get(0)).getRootName();
        ExprType fieldType = helper.fieldTypes.get(fieldName);

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
        throw new AggregateAnalyzerException(
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

  private static ValuesSourceAggregationBuilder<?> createBucketAggregation(
      Integer group, Project project, AggregateAnalyzer.AggregateBuilderHelper helper) {
    return createBucket(group, project, helper);
  }

  private static List<CompositeValuesSourceBuilder<?>> createCompositeBuckets(
      List<Integer> groupList, Project project, AggregateAnalyzer.AggregateBuilderHelper helper) {
    ImmutableList.Builder<CompositeValuesSourceBuilder<?>> resultBuilder = ImmutableList.builder();
    groupList.forEach(
        groupIndex -> resultBuilder.add(createCompositeBucket(groupIndex, project, helper)));
    return resultBuilder.build();
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
    } else {
      return createTermsAggregationBuilder(bucketName, rex, helper);
    }
  }

  private static CompositeValuesSourceBuilder<?> createCompositeBucket(
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
      return CompositeAggregationBuilder.buildHistogram(
          bucketName,
          helper.inferNamedField(((RexCall) rex).getOperands().get(0)).getRootName(),
          ((RexLiteral)((RexCall) rex).getOperands().get(1)).getValueAs(Double.class),
          SpanUnit.of(((RexLiteral)((RexCall) rex).getOperands().get(2)).getValueAs(String.class)),
          MissingOrder.FIRST,
          helper.bucketNullable);
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

    // Time types values are converted to LONG in ExpressionAggregationScript::execute
    if (List.of(TIMESTAMP, TIME, DATE)
        .contains(OpenSearchTypeFactory.convertRelDataTypeToExprType(group.getType()))) {
      sourceBuilder.userValuetypeHint(ValueType.LONG);
    }

    return sourceBuilder;
  }

  private static ValuesSourceAggregationBuilder<?> createTermsAggregationBuilder(
      String bucketName, RexNode group, AggregateBuilderHelper helper) {
    TermsAggregationBuilder sourceBuilder =
        helper.build(
            group,
            new TermsAggregationBuilder(bucketName)
                .size(AGGREGATION_BUCKET_SIZE)
                .order(BucketOrder.key(true)));
    // Time types values are converted to LONG in ExpressionAggregationScript::execute
    if (List.of(TIMESTAMP, TIME, DATE)
        .contains(OpenSearchTypeFactory.convertRelDataTypeToExprType(group.getType()))) {
      sourceBuilder.userValueTypeHint(ValueType.LONG);
    }

    return sourceBuilder;
  }
}

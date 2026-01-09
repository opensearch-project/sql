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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
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
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.NamedFieldExpression;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.ScriptQueryExpression;
import org.opensearch.sql.opensearch.response.agg.ArgMaxMinParser;
import org.opensearch.sql.opensearch.response.agg.BucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.CountAsTotalHitsParser;
import org.opensearch.sql.opensearch.response.agg.MetricParser;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SinglePercentileParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.response.agg.StatsParser;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;
import org.opensearch.sql.opensearch.storage.script.aggregation.dsl.CompositeAggregationBuilder;
import org.opensearch.sql.utils.Utils;

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

  private AggregateAnalyzer() {}

  @RequiredArgsConstructor
  public static class AggregateBuilderHelper {
    final RelDataType rowType;
    final Map<String, ExprType> fieldTypes;
    final RelOptCluster cluster;
    final boolean bucketNullable;
    final int queryBucketSize;

    <T extends ValuesSourceAggregationBuilder<T>> T build(RexNode node, T aggBuilder) {
      return build(node, aggBuilder::field, aggBuilder::script);
    }

    <T extends CompositeValuesSourceBuilder<T>> T build(RexNode node, T sourceBuilder) {
      return build(node, sourceBuilder::field, sourceBuilder::script);
    }

    <T> T build(RexNode node, Function<String, T> fieldBuilder, Function<Script, T> scriptBuilder) {
      if (node == null) return fieldBuilder.apply(METADATA_FIELD);
      else if (node instanceof RexInputRef ref) {
        return fieldBuilder.apply(inferNamedField(node).getReferenceForTermQuery());
      } else if (node instanceof RexCall || node instanceof RexLiteral) {
        return scriptBuilder.apply(inferScript(node).getScript());
      }
      throw new IllegalStateException(
          String.format("Metric aggregation doesn't support RexNode %s", node));
    }

    NamedFieldExpression inferNamedField(RexNode node) {
      if (node instanceof RexInputRef ref) {
        return new NamedFieldExpression(ref.getIndex(), rowType.getFieldNames(), fieldTypes);
      }
      throw new IllegalStateException(
          String.format("Cannot infer field name from RexNode %s", node));
    }

    ScriptQueryExpression inferScript(RexNode node) {
      if (node instanceof RexCall || node instanceof RexLiteral) {
        return new ScriptQueryExpression(
            node, rowType, fieldTypes, cluster, Collections.emptyMap());
      }
      throw new IllegalStateException(
          String.format("Metric aggregation doesn't support RexNode %s", node));
    }

    <T> T inferValue(RexNode node, Class<T> clazz) {
      if (node instanceof RexLiteral literal) {
        return literal.getValueAs(clazz);
      }
      throw new IllegalStateException(String.format("Cannot infer value from RexNode %s", node));
    }

    RexNode inferRexNodeFromIndex(int index, Project project) {
      return project == null ? RexInputRef.of(index, rowType) : project.getProjects().get(index);
    }

    String inferFieldNameFromIndex(int index, Project project) {
      return project == null
          ? rowType.getFieldNames().get(index)
          : project.getRowType().getFieldNames().get(index);
    }
  }

  // TODO: should we support filter aggregation? For PPL, we don't have filter in stats command
  public static Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> analyze(
      Aggregate aggregate,
      @Nullable Project project,
      final List<String> outputFields,
      AggregateBuilderHelper helper)
      throws ExpressionNotAnalyzableException {
    requireNonNull(aggregate, "aggregate");
    try {
      final List<Integer> groupList = aggregate.getGroupSet().asList();
      List<String> aggFieldNames = outputFields.subList(groupList.size(), outputFields.size());
      // Process all aggregate calls
      Pair<Builder, List<MetricParser>> builderAndParser =
          processAggregateCalls(aggFieldNames, aggregate.getAggCallList(), project, helper);
      Builder metricBuilder = builderAndParser.getLeft();
      List<MetricParser> metricParsers = builderAndParser.getRight();

      // both count() and count(FIELD) can apply doc_count optimization in non-bucket aggregation,
      // but only count() can apply doc_count optimization in bucket aggregation.
      boolean countAllOnly = !groupList.isEmpty();
      Pair<List<String>, Builder> countAggNameAndBuilderPair =
          removeCountAggregationBuilders(metricBuilder, countAllOnly);
      Builder newMetricBuilder = countAggNameAndBuilderPair.getRight();
      List<String> countAggNames = countAggNameAndBuilderPair.getLeft();

      // No group-by clause -- no parent aggregations are attached:
      //   - stats count()
      //   - stats avg(), count()
      // Metric
      if (aggregate.getGroupSet().isEmpty()) {
        if (newMetricBuilder == null) {
          // The optimization must require all count aggregations are removed,
          // and they have only one field name
          return Pair.of(List.of(), new CountAsTotalHitsParser(countAggNames));
        } else {
          return Pair.of(
              ImmutableList.copyOf(newMetricBuilder.getAggregatorFactories()),
              new NoBucketAggregationParser(metricParsers));
        }
      } else {
        // Used to track the current sub-builder as analysis progresses
        Builder subBuilder = newMetricBuilder;
        // Push auto date span & case in group-by list into structured aggregations
        List<Pair<String, Integer>> groupNameAndIndexList =
            IntStream.range(0, groupList.size())
                .mapToObj(i -> Pair.of(outputFields.get(i), groupList.get(i)))
                .toList();
        Pair<Set<Integer>, AggregationBuilder> aggPushedAndAggBuilder =
            createStructuredAggregation(groupNameAndIndexList, project, subBuilder, helper);
        Set<Integer> aggPushed = aggPushedAndAggBuilder.getLeft();
        AggregationBuilder pushedAggBuilder = aggPushedAndAggBuilder.getRight();
        // The group-by list after removing pushed aggregations
        groupNameAndIndexList =
            groupNameAndIndexList.stream()
                .filter(pair -> !aggPushed.contains(pair.getRight()))
                .toList();
        if (pushedAggBuilder != null) {
          subBuilder = new Builder().addAggregator(pushedAggBuilder);
        }

        // No composite aggregation at top-level -- auto date span & case in group-by list are
        // pushed into structured aggregations:
        //   - stats avg() by range_field
        //   - stats count() by auto_date_span
        //   - stats count() by ...auto_date_spans, ...range_fields
        // [AutoDateHistogram | RangeAgg]+
        //   Metric
        if (groupNameAndIndexList.isEmpty()) {
          return Pair.of(
              ImmutableList.copyOf(subBuilder.getAggregatorFactories()),
              new BucketAggregationParser(metricParsers, countAggNames));
        }
        // Composite aggregation at top level -- it has composite aggregation, with or without its
        // incompatible value sources as sub-aggregations:
        //   - stats avg() by term_fields
        //   - stats avg() by date_histogram
        //   - stats count() by auto_date_span, range_field, term_fields
        // CompositeAgg
        //   [AutoDateHistogram | RangeAgg]*
        //     Metric
        else {
          List<CompositeValuesSourceBuilder<?>> buckets =
              createCompositeBuckets(groupNameAndIndexList, project, helper);
          if (buckets.size() != groupNameAndIndexList.size()) {
            throw new UnsupportedOperationException(
                "Not all the left aggregations can be converted to value sources of composite"
                    + " aggregation");
          }
          AggregationBuilder compositeBuilder =
              AggregationBuilders.composite("composite_buckets", buckets)
                  .size(helper.queryBucketSize);
          if (subBuilder != null) {
            compositeBuilder.subAggregations(subBuilder);
          }
          List<String> nestedPathFromBuckets =
              groupNameAndIndexList.stream()
                  .map(b -> Utils.resolveNestedPath(b.getLeft(), helper.fieldTypes))
                  .toList();
          boolean validNestedAgg =
              nestedPathFromBuckets.stream().noneMatch(Objects::isNull)
                  && nestedPathFromBuckets.stream().distinct().count() == 1;
          if (validNestedAgg) {
            String nestedPath = nestedPathFromBuckets.getFirst();
            AggregationBuilder nestedAggBuilder =
                AggregationBuilders.nested(
                        String.format("nested_%s", compositeBuilder.getName()), nestedPath)
                    .subAggregation(compositeBuilder);
            return Pair.of(
                Collections.singletonList(nestedAggBuilder),
                new BucketAggregationParser(metricParsers, countAggNames));
          }
          return Pair.of(
              Collections.singletonList(compositeBuilder),
              new BucketAggregationParser(metricParsers, countAggNames));
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
            .toList();
    List<AggregationBuilder> copy = new ArrayList<>(metricBuilder.getAggregatorFactories());
    copy.removeAll(countAggregatorFactories);
    Builder newMetricBuilder = new AggregatorFactories.Builder();
    copy.forEach(newMetricBuilder::addAggregator);

    if (countAllOnly || supportCountFiled(countAggregatorFactories, metricBuilder)) {
      List<String> countAggNameList =
          countAggregatorFactories.stream().map(ValuesSourceAggregationBuilder::getName).toList();
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
      List<String> aggNames,
      List<AggregateCall> aggCalls,
      Project project,
      AggregateAnalyzer.AggregateBuilderHelper helper)
      throws PredicateAnalyzer.ExpressionNotAnalyzableException {
    Builder metricBuilder = new AggregatorFactories.Builder();
    List<MetricParser> metricParserList = new ArrayList<>();
    AggregateFilterAnalyzer aggFilterAnalyzer = new AggregateFilterAnalyzer(helper, project);

    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      List<Pair<RexNode, String>> args = convertAggArgThroughProject(aggCall, project, helper);
      String aggName = aggNames.get(i);

      Pair<AggregationBuilder, MetricParser> builderAndParser =
          createAggregationBuilderAndParser(aggCall, args, aggName, helper);
      builderAndParser = aggFilterAnalyzer.analyze(builderAndParser, aggCall, aggName);
      // Nested aggregation (https://docs.opensearch.org/docs/latest/aggregations/bucket/nested/)
      String nestedPath =
          args.isEmpty()
              ? null
              : Utils.resolveNestedPath(args.getFirst().getRight(), helper.fieldTypes);
      if (nestedPath != null) {
        metricBuilder.addAggregator(
            AggregationBuilders.nested(String.format("nested_%s", aggCall.getName()), nestedPath)
                .subAggregation(builderAndParser.getLeft()));
      } else {
        metricBuilder.addAggregator(builderAndParser.getLeft());
      }
      metricParserList.add(builderAndParser.getRight());
    }
    return Pair.of(metricBuilder, metricParserList);
  }

  /**
   * Convert aggregate arguments through child project. Normally, just return the rex nodes of
   * Project which are included in aggCall expression. If the aggCall is a LITERAL_AGG, it returns
   * all rex nodes of Project except WindowFunction.
   *
   * @param aggCall the aggregate call
   * @param project the project
   * @param helper the AggregateBuilderHelper
   * @return the converted Pair<RexNode, String> list
   */
  private static List<Pair<RexNode, String>> convertAggArgThroughProject(
      AggregateCall aggCall, Project project, AggregateAnalyzer.AggregateBuilderHelper helper) {
    return project == null
        ? aggCall.getArgList().stream()
            .map(
                i ->
                    Pair.of(
                        (RexNode) RexInputRef.of(i, helper.rowType),
                        helper.rowType.getFieldNames().get(i)))
            .toList()
        : PlanUtils.getObjectFromLiteralAgg(aggCall) != null
            ? project.getNamedProjects().stream()
                .filter(rex -> !rex.getKey().isA(SqlKind.ROW_NUMBER))
                .map(p -> Pair.of(p.getKey(), p.getValue()))
                .toList()
            : aggCall.getArgList().stream()
                .map(project.getNamedProjects()::get)
                .map(p -> Pair.of(p.getKey(), p.getValue()))
                .toList();
  }

  private static Pair<AggregationBuilder, MetricParser> createAggregationBuilderAndParser(
      AggregateCall aggCall,
      List<Pair<RexNode, String>> args,
      String aggName,
      AggregateAnalyzer.AggregateBuilderHelper helper) {
    if (aggCall.isDistinct()) {
      return createDistinctAggregation(aggCall, args, aggName, helper);
    } else {
      return createRegularAggregation(aggCall, args, aggName, helper);
    }
  }

  private static Pair<AggregationBuilder, MetricParser> createDistinctAggregation(
      AggregateCall aggCall,
      List<Pair<RexNode, String>> args,
      String aggName,
      AggregateBuilderHelper helper) {

    return switch (aggCall.getAggregation().kind) {
      case COUNT ->
          Pair.of(
              helper.build(
                  !args.isEmpty() ? args.getFirst().getKey() : null,
                  AggregationBuilders.cardinality(aggName)),
              new SingleValueParser(aggName));
      default ->
          throw new AggregateAnalyzer.AggregateAnalyzerException(
              String.format("unsupported distinct aggregator %s", aggCall.getAggregation()));
    };
  }

  private static Pair<AggregationBuilder, MetricParser> createRegularAggregation(
      AggregateCall aggCall,
      List<Pair<RexNode, String>> args,
      String aggName,
      AggregateBuilderHelper helper) {

    return switch (aggCall.getAggregation().kind) {
      case AVG ->
          Pair.of(
              helper.build(args.getFirst().getKey(), AggregationBuilders.avg(aggName)),
              new SingleValueParser(aggName));
      // 1. Only case SUM, skip SUM0 / COUNT since calling avg() in DSL should be faster.
      // 2. To align with databases, SUM0 is not preferred now.
      case SUM ->
          Pair.of(
              helper.build(args.getFirst().getKey(), AggregationBuilders.sum(aggName)),
              new SingleValueParser(aggName));
      case COUNT ->
          Pair.of(
              helper.build(
                  !args.isEmpty() ? args.getFirst().getKey() : null,
                  AggregationBuilders.count(aggName)),
              new SingleValueParser(aggName));
      case MIN -> {
        ExprType fieldType =
            OpenSearchTypeFactory.convertRelDataTypeToExprType(args.getFirst().getKey().getType());
        if (supportsMaxMinAggregation(fieldType)) {
          yield Pair.of(
              helper.build(args.getFirst().getKey(), AggregationBuilders.min(aggName)),
              new SingleValueParser(aggName));
        } else {
          yield Pair.of(
              AggregationBuilders.topHits(aggName)
                  .fetchField(
                      helper.inferNamedField(args.getFirst().getKey()).getReferenceForTermQuery())
                  .size(1)
                  .from(0)
                  .sort(
                      helper.inferNamedField(args.getFirst().getKey()).getReferenceForTermQuery(),
                      SortOrder.ASC),
              new TopHitsParser(aggName, true, false));
        }
      }
      case MAX -> {
        ExprType fieldType =
            OpenSearchTypeFactory.convertRelDataTypeToExprType(args.getFirst().getKey().getType());
        if (supportsMaxMinAggregation(fieldType)) {
          yield Pair.of(
              helper.build(args.getFirst().getKey(), AggregationBuilders.max(aggName)),
              new SingleValueParser(aggName));
        } else {
          yield Pair.of(
              AggregationBuilders.topHits(aggName)
                  .fetchField(
                      helper.inferNamedField(args.getFirst().getKey()).getReferenceForTermQuery())
                  .size(1)
                  .from(0)
                  .sort(
                      helper.inferNamedField(args.getFirst().getKey()).getReferenceForTermQuery(),
                      SortOrder.DESC),
              new TopHitsParser(aggName, true, false));
        }
      }
      case VAR_SAMP ->
          Pair.of(
              helper.build(args.getFirst().getKey(), AggregationBuilders.extendedStats(aggName)),
              new StatsParser(ExtendedStats::getVarianceSampling, aggName));
      case VAR_POP ->
          Pair.of(
              helper.build(args.getFirst().getKey(), AggregationBuilders.extendedStats(aggName)),
              new StatsParser(ExtendedStats::getVariancePopulation, aggName));
      case STDDEV_SAMP ->
          Pair.of(
              helper.build(args.getFirst().getKey(), AggregationBuilders.extendedStats(aggName)),
              new StatsParser(ExtendedStats::getStdDeviationSampling, aggName));
      case STDDEV_POP ->
          Pair.of(
              helper.build(args.getFirst().getKey(), AggregationBuilders.extendedStats(aggName)),
              new StatsParser(ExtendedStats::getStdDeviationPopulation, aggName));
      case ARG_MAX ->
          Pair.of(
              AggregationBuilders.topHits(aggName)
                  .fetchField(
                      helper.inferNamedField(args.getFirst().getKey()).getReferenceForTermQuery())
                  .size(1)
                  .from(0)
                  .sort(
                      helper.inferNamedField(args.get(1).getKey()).getReferenceForTermQuery(),
                      org.opensearch.search.sort.SortOrder.DESC),
              new ArgMaxMinParser(aggName));
      case ARG_MIN ->
          Pair.of(
              AggregationBuilders.topHits(aggName)
                  .fetchField(
                      helper.inferNamedField(args.getFirst().getKey()).getReferenceForTermQuery())
                  .size(1)
                  .from(0)
                  .sort(
                      helper.inferNamedField(args.get(1).getKey()).getReferenceForTermQuery(),
                      org.opensearch.search.sort.SortOrder.ASC),
              new ArgMaxMinParser(aggName));
      case OTHER_FUNCTION -> {
        BuiltinFunctionName functionName =
            BuiltinFunctionName.ofAggregation(aggCall.getAggregation().getName()).get();
        yield switch (functionName) {
          case TAKE ->
              Pair.of(
                  AggregationBuilders.topHits(aggName)
                      .fetchField(
                          helper
                              .inferNamedField(args.getFirst().getKey())
                              .getReferenceForTermQuery())
                      .size(helper.inferValue(args.getLast().getKey(), Integer.class))
                      .from(0),
                  new TopHitsParser(aggName, false, true));
          case FIRST -> {
            TopHitsAggregationBuilder firstBuilder =
                AggregationBuilders.topHits(aggName).size(1).from(0);
            if (!args.isEmpty()) {
              firstBuilder.fetchField(
                  helper.inferNamedField(args.getFirst().getKey()).getReferenceForTermQuery());
            }
            yield Pair.of(firstBuilder, new TopHitsParser(aggName, true, false));
          }
          case LAST -> {
            TopHitsAggregationBuilder lastBuilder =
                AggregationBuilders.topHits(aggName)
                    .size(1)
                    .from(0)
                    .sort("_doc", org.opensearch.search.sort.SortOrder.DESC);
            if (!args.isEmpty()) {
              lastBuilder.fetchField(
                  helper.inferNamedField(args.getFirst().getKey()).getReferenceForTermQuery());
            }
            yield Pair.of(lastBuilder, new TopHitsParser(aggName, true, false));
          }
          case PERCENTILE_APPROX -> {
            PercentilesAggregationBuilder aggBuilder =
                helper
                    .build(args.getFirst().getKey(), AggregationBuilders.percentiles(aggName))
                    .percentiles(helper.inferValue(args.get(1).getKey(), Double.class));
            /* See {@link PercentileApproxFunction}, PERCENTILE_APPROX accepts args of [FIELD, PERCENTILE, TYPE, COMPRESSION(optional)] */
            if (args.size() > 3) {
              aggBuilder.compression(helper.inferValue(args.getLast().getKey(), Double.class));
            }
            yield Pair.of(aggBuilder, new SinglePercentileParser(aggName));
          }
          case DISTINCT_COUNT_APPROX ->
              Pair.of(
                  helper.build(
                      !args.isEmpty() ? args.getFirst().getKey() : null,
                      AggregationBuilders.cardinality(aggName)),
                  new SingleValueParser(aggName));
          default ->
              throw new AggregateAnalyzer.AggregateAnalyzerException(
                  String.format("Unsupported push-down aggregator %s", aggCall.getAggregation()));
        };
      }
      case LITERAL_AGG -> {
        RexLiteral literal = PlanUtils.getObjectFromLiteralAgg(aggCall);
        if (literal == null || !(literal.getValue() instanceof Number)) {
          throw new AggregateAnalyzer.AggregateAnalyzerException(
              String.format("Unsupported push-down aggregator %s", aggCall.getAggregation()));
        }
        Integer dedupNumber = literal.getValueAs(Integer.class);
        // Disable fetchSource since TopHitsParser only parses fetchField currently.
        TopHitsAggregationBuilder topHitsAggregationBuilder =
            AggregationBuilders.topHits(aggName).from(0).size(dedupNumber);
        List<String> sources = new ArrayList<>();
        List<SearchSourceBuilder.ScriptField> scripts = new ArrayList<>();
        args.forEach(
            rex -> {
              if (rex.getKey() instanceof RexInputRef) {
                sources.add(helper.inferNamedField(rex.getKey()).getReference());
              } else if (rex.getKey() instanceof RexCall || rex.getKey() instanceof RexLiteral) {
                scripts.add(
                    new SearchSourceBuilder.ScriptField(
                        rex.getValue(), helper.inferScript(rex.getKey()).getScript(), false));
              } else {
                throw new AggregateAnalyzer.AggregateAnalyzerException(
                    String.format(
                        "Unsupported push-down aggregator %s due to rex type is %s",
                        aggCall.getAggregation(), rex.getKey().getKind()));
              }
            });
        topHitsAggregationBuilder.fetchSource(
            sources.stream().distinct().toArray(String[]::new), new String[0]);
        topHitsAggregationBuilder.scriptFields(scripts);
        yield Pair.of(topHitsAggregationBuilder, new TopHitsParser(aggName, false, false));
      }
      default ->
          throw new AggregateAnalyzer.AggregateAnalyzerException(
              String.format("unsupported aggregator %s", aggCall.getAggregation()));
    };
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
      List<Pair<String, Integer>> groupNameAndIndexList,
      @Nullable Project project,
      AggregateAnalyzer.AggregateBuilderHelper helper) {
    ImmutableList.Builder<CompositeValuesSourceBuilder<?>> resultBuilder = ImmutableList.builder();
    groupNameAndIndexList.forEach(
        nameAndIndex ->
            resultBuilder.add(
                createCompositeBucket(
                    nameAndIndex.getLeft(), nameAndIndex.getRight(), project, helper)));
    return resultBuilder.build();
  }

  /**
   * Creates structured bucket aggregations for expressions that are not qualified as value sources
   * for composite aggregations.
   *
   * <p>This method processes a list of group by expressions and identifies those that cannot be
   * used as value sources in composite aggregations but can be pushed down as sub-aggregations,
   * such as auto date histograms and range buckets.
   *
   * <p>The aggregation hierarchy follows this pattern:
   *
   * <pre>
   * AutoDateHistogram | RangeAggregation
   *   └── AutoDateHistogram | RangeAggregation (structured)
   *       └── ... (more composite-incompatible aggregations)
   *           └── Metric Aggregation (at the bottom)
   * </pre>
   *
   * @param groupNameAndIndexList the list of group by field names and indices from the query
   * @param project the projection containing the expressions to analyze
   * @param metricBuilder the metric aggregation builder to be placed at the bottom of the hierarchy
   * @param helper the aggregation builder helper containing row type and utility methods
   * @return a pair containing:
   *     <ul>
   *       <li>A set of integers representing the indices of group fields that were successfully
   *           pushed as sub-aggregations
   *       <li>The root aggregation builder, or null if no such expressions were found
   *     </ul>
   */
  private static Pair<Set<Integer>, AggregationBuilder> createStructuredAggregation(
      List<Pair<String, Integer>> groupNameAndIndexList,
      @Nullable Project project,
      Builder metricBuilder,
      AggregateAnalyzer.AggregateBuilderHelper helper) {
    AggregationBuilder rootAggBuilder = null;
    AggregationBuilder tailAggBuilder = null;

    Set<Integer> aggPushed = new HashSet<>();
    for (Pair<String, Integer> nameAndIndex : groupNameAndIndexList) {
      RexNode agg = helper.inferRexNodeFromIndex(nameAndIndex.getRight(), project);
      String name = nameAndIndex.getLeft();
      AggregationBuilder aggBuilder = createCompositeIncompatibleAggregation(agg, name, helper);
      if (aggBuilder != null) {
        aggPushed.add(nameAndIndex.getRight());
        if (rootAggBuilder == null) {
          rootAggBuilder = aggBuilder;
        } else {
          tailAggBuilder.subAggregation(aggBuilder);
        }
        tailAggBuilder = aggBuilder;
      }
    }
    if (tailAggBuilder != null && metricBuilder != null) {
      tailAggBuilder.subAggregations(metricBuilder);
    }
    return Pair.of(aggPushed, rootAggBuilder);
  }

  /**
   * Creates an aggregation builder for expressions that are not qualified as composite aggregation
   * value sources.
   *
   * <p>This method analyzes RexNode expressions and creates appropriate OpenSearch aggregation
   * builders for cases where they can not be value sources of a composite aggregation.
   *
   * <p>The method supports the following aggregation types:
   *
   * <pre>
   * - Auto Date Histogram Aggregation: For temporal bucketing with automatic interval selection
   * - Range Aggregation: For CASE expressions that define value ranges
   * </pre>
   *
   * @param agg the RexNode expression to analyze and convert
   * @param name the name to assign to the created aggregation builder
   * @param helper the aggregation builder helper containing row type and utility methods
   * @return the appropriate ValuesSourceAggregationBuilder for the expression, or null if no
   *     compatible aggregation type is found
   */
  private static ValuesSourceAggregationBuilder<?> createCompositeIncompatibleAggregation(
      RexNode agg, String name, AggregateBuilderHelper helper) {
    ValuesSourceAggregationBuilder<?> aggBuilder = null;
    if (isAutoDateSpan(agg)) {
      aggBuilder = analyzeAutoDateSpan(agg, name, helper);
    } else if (isCase(agg)) {
      Optional<RangeAggregationBuilder> rangeAggBuilder =
          CaseRangeAnalyzer.create(name, helper.rowType).analyze((RexCall) agg);
      if (rangeAggBuilder.isPresent()) {
        aggBuilder = rangeAggBuilder.get();
      }
    }
    return aggBuilder;
  }

  private static AutoDateHistogramAggregationBuilder analyzeAutoDateSpan(
      RexNode spanAgg, String name, AggregateAnalyzer.AggregateBuilderHelper helper) {
    RexCall rexCall = (RexCall) spanAgg;
    RexInputRef rexInputRef = (RexInputRef) rexCall.getOperands().getFirst();
    RexLiteral valueLiteral = (RexLiteral) rexCall.getOperands().get(1);
    return new AutoDateHistogramAggregationBuilder(name)
        .field(helper.inferNamedField(rexInputRef).getRootName())
        .setNumBuckets(requireNonNull(valueLiteral.getValueAs(Integer.class)));
  }

  private static boolean isAutoDateSpan(RexNode rex) {
    return rex instanceof RexCall rexCall
        && rexCall.getKind() == SqlKind.OTHER_FUNCTION
        && rexCall.getOperator().equals(WIDTH_BUCKET);
  }

  private static boolean isCase(RexNode rex) {
    return rex instanceof RexCall rexCall && rexCall.getKind() == SqlKind.CASE;
  }

  private static CompositeValuesSourceBuilder<?> createCompositeBucket(
      String bucketName,
      Integer groupIndex,
      @Nullable Project project,
      AggregateAnalyzer.AggregateBuilderHelper helper) {
    RexNode rex = helper.inferRexNodeFromIndex(groupIndex, project);
    if (rex instanceof RexCall rexCall
        && rexCall.getKind() == SqlKind.OTHER_FUNCTION
        && rexCall.getOperator().getName().equalsIgnoreCase(BuiltinFunctionName.SPAN.name())
        && rexCall.getOperands().size() == 3
        && rexCall.getOperands().getFirst() instanceof RexInputRef rexInputRef
        && rexCall.getOperands().get(1) instanceof RexLiteral valueLiteral
        && rexCall.getOperands().get(2) instanceof RexLiteral unitLiteral) {
      return CompositeAggregationBuilder.buildHistogram(
          bucketName,
          helper.inferNamedField(rexInputRef).getRootName(),
          valueLiteral.getValueAs(Double.class),
          SpanUnit.of(unitLiteral.getValueAs(String.class)),
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
                .size(helper.queryBucketSize)
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import static org.opensearch.search.aggregations.MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/** Immutable aggregation pushdown specification used during planning. */
@Getter
public final class AggSpec {
  private enum AggKind {
    OTHER,
    COMPOSITE,
    TERMS,
    MULTI_TERMS,
    DATE_HISTOGRAM,
    HISTOGRAM,
    TOP_HITS,
    RARE_TOP
  }

  private final Aggregate aggregate;
  @Nullable private final Project project;
  private final List<String> outputFields;
  private final RelDataType rowType;
  private final Map<String, ExprType> fieldTypes;
  private final RelOptCluster cluster;
  private final boolean bucketNullable;
  private final int queryBucketSize;
  private final Map<String, OpenSearchDataType> extendedTypeMapping;
  private final List<String> initialBucketNames;
  private final List<String> bucketNames;
  private final long scriptCount;
  private final AggKind kind;
  @Nullable private final AggKind measureSortTarget;
  private final boolean rareTopSupported;
  @Nullable private final List<RelFieldCollation> bucketSortCollations;
  @Nullable private final List<String> bucketSortFieldNames;
  @Nullable private final List<RelFieldCollation> measureSortCollations;
  @Nullable private final List<String> measureSortFieldNames;
  @Nullable private final RareTopDigest rareTopDigest;
  @Nullable private final Integer bucketSize;

  private AggSpec(
      Aggregate aggregate,
      @Nullable Project project,
      List<String> outputFields,
      RelDataType rowType,
      Map<String, ExprType> fieldTypes,
      RelOptCluster cluster,
      boolean bucketNullable,
      int queryBucketSize,
      Map<String, OpenSearchDataType> extendedTypeMapping,
      List<String> initialBucketNames,
      List<String> bucketNames,
      long scriptCount,
      AggKind kind,
      @Nullable AggKind measureSortTarget,
      boolean rareTopSupported,
      @Nullable List<RelFieldCollation> bucketSortCollations,
      @Nullable List<String> bucketSortFieldNames,
      @Nullable List<RelFieldCollation> measureSortCollations,
      @Nullable List<String> measureSortFieldNames,
      @Nullable RareTopDigest rareTopDigest,
      @Nullable Integer bucketSize) {
    this.aggregate = aggregate;
    this.project = project;
    this.outputFields = List.copyOf(outputFields);
    this.rowType = rowType;
    this.fieldTypes = Map.copyOf(fieldTypes);
    this.cluster = cluster;
    this.bucketNullable = bucketNullable;
    this.queryBucketSize = queryBucketSize;
    this.extendedTypeMapping = Map.copyOf(extendedTypeMapping);
    this.initialBucketNames = List.copyOf(initialBucketNames);
    this.bucketNames = List.copyOf(bucketNames);
    this.scriptCount = scriptCount;
    this.kind = kind;
    this.measureSortTarget = measureSortTarget;
    this.rareTopSupported = rareTopSupported;
    this.bucketSortCollations =
        bucketSortCollations == null ? null : List.copyOf(bucketSortCollations);
    this.bucketSortFieldNames =
        bucketSortFieldNames == null ? null : List.copyOf(bucketSortFieldNames);
    this.measureSortCollations =
        measureSortCollations == null ? null : List.copyOf(measureSortCollations);
    this.measureSortFieldNames =
        measureSortFieldNames == null ? null : List.copyOf(measureSortFieldNames);
    this.rareTopDigest = rareTopDigest;
    this.bucketSize = bucketSize;
  }

  public static AggSpec create(
      Aggregate aggregate,
      @Nullable Project project,
      List<String> outputFields,
      RelDataType rowType,
      Map<String, ExprType> fieldTypes,
      RelOptCluster cluster,
      boolean bucketNullable,
      int queryBucketSize,
      Map<String, OpenSearchDataType> extendedTypeMapping,
      List<String> bucketNames,
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> builderAndParser) {
    AggregationBuilder rootBuilder =
        builderAndParser.getLeft().isEmpty() ? null : builderAndParser.getLeft().getFirst();
    AggKind kind = inferKind(rootBuilder);
    return new AggSpec(
        aggregate,
        project,
        outputFields,
        rowType,
        fieldTypes,
        cluster,
        bucketNullable,
        queryBucketSize,
        extendedTypeMapping,
        bucketNames,
        bucketNames,
        new AggPushDownAction(builderAndParser, extendedTypeMapping, bucketNames).getScriptCount(),
        kind,
        inferMeasureSortTarget(rootBuilder),
        isRareTopSupported(rootBuilder),
        null,
        null,
        null,
        null,
        null,
        inferBucketSize(rootBuilder));
  }

  public boolean isCompositeAggregation() {
    return kind == AggKind.COMPOSITE;
  }

  public boolean isSingleRowAggregation() {
    return aggregate.getGroupSet().isEmpty();
  }

  public boolean canPushDownLimitIntoBucketSize(int size) {
    return bucketSize != null && size < bucketSize;
  }

  public AggSpec withBucketSort(List<RelFieldCollation> collations, List<String> fieldNames) {
    if (kind != AggKind.COMPOSITE && kind != AggKind.TERMS) {
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot pushdown sort into aggregation bucket");
    }
    List<String> newBucketNames = bucketNames;
    if (kind == AggKind.COMPOSITE) {
      List<String> reordered = new ArrayList<>(bucketNames.size());
      List<String> selected = new ArrayList<>(collations.size());
      for (RelFieldCollation collation : collations) {
        String bucketName = fieldNames.get(collation.getFieldIndex());
        if (!bucketNames.contains(bucketName)) {
          throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
              "Cannot pushdown sort into aggregation bucket");
        }
        reordered.add(bucketName);
        selected.add(bucketName);
      }
      for (String name : bucketNames) {
        if (!selected.contains(name)) {
          reordered.add(name);
        }
      }
      newBucketNames = reordered;
    }
    return new AggSpec(
        aggregate,
        project,
        outputFields,
        rowType,
        fieldTypes,
        cluster,
        bucketNullable,
        queryBucketSize,
        extendedTypeMapping,
        initialBucketNames,
        newBucketNames,
        scriptCount,
        kind,
        measureSortTarget,
        rareTopSupported,
        collations,
        fieldNames,
        measureSortCollations,
        measureSortFieldNames,
        rareTopDigest,
        bucketSize);
  }

  public AggSpec withoutBucketSort() {
    if (bucketSortCollations == null) {
      return this;
    }
    return new AggSpec(
        aggregate,
        project,
        outputFields,
        rowType,
        fieldTypes,
        cluster,
        bucketNullable,
        queryBucketSize,
        extendedTypeMapping,
        initialBucketNames,
        initialBucketNames,
        scriptCount,
        kind,
        measureSortTarget,
        rareTopSupported,
        null,
        null,
        measureSortCollations,
        measureSortFieldNames,
        rareTopDigest,
        bucketSize);
  }

  public AggSpec withSortMeasure(List<RelFieldCollation> collations, List<String> fieldNames) {
    if (kind != AggKind.COMPOSITE || measureSortTarget == null) {
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot pushdown sort aggregate measure");
    }
    Integer resizedBucketSize =
        switch (measureSortTarget) {
          case TERMS, MULTI_TERMS -> bucketSize;
          default -> null;
        };
    return new AggSpec(
        aggregate,
        project,
        outputFields,
        rowType,
        fieldTypes,
        cluster,
        bucketNullable,
        queryBucketSize,
        extendedTypeMapping,
        initialBucketNames,
        bucketNames,
        scriptCount,
        measureSortTarget,
        null,
        rareTopSupported,
        bucketSortCollations,
        bucketSortFieldNames,
        collations,
        fieldNames,
        rareTopDigest,
        resizedBucketSize);
  }

  public AggSpec withRareTop(RareTopDigest digest) {
    if (kind != AggKind.COMPOSITE || !rareTopSupported) {
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException("Cannot pushdown " + digest);
    }
    return new AggSpec(
        aggregate,
        project,
        outputFields,
        rowType,
        fieldTypes,
        cluster,
        bucketNullable,
        queryBucketSize,
        extendedTypeMapping,
        initialBucketNames,
        bucketNames,
        scriptCount,
        AggKind.RARE_TOP,
        null,
        rareTopSupported,
        bucketSortCollations,
        bucketSortFieldNames,
        measureSortCollations,
        measureSortFieldNames,
        digest,
        digest.byList().isEmpty() ? digest.number() : DEFAULT_MAX_BUCKETS);
  }

  public AggSpec withLimit(int size) {
    if (!canPushDownLimitIntoBucketSize(size)) {
      return this;
    }
    return new AggSpec(
        aggregate,
        project,
        outputFields,
        rowType,
        fieldTypes,
        cluster,
        bucketNullable,
        queryBucketSize,
        extendedTypeMapping,
        initialBucketNames,
        bucketNames,
        scriptCount,
        kind,
        measureSortTarget,
        rareTopSupported,
        bucketSortCollations,
        bucketSortFieldNames,
        measureSortCollations,
        measureSortFieldNames,
        rareTopDigest,
        size);
  }

  public Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> build() {
    try {
      AggregateAnalyzer.AggregateBuilderHelper helper =
          new AggregateAnalyzer.AggregateBuilderHelper(
              rowType, fieldTypes, cluster, bucketNullable, queryBucketSize);
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> builderAndParser =
          AggregateAnalyzer.analyze(aggregate, project, outputFields, helper);
      AggPushDownAction temp =
          new AggPushDownAction(
              builderAndParser, extendedTypeMapping, new ArrayList<>(initialBucketNames));
      if (bucketSortCollations != null) {
        temp.pushDownSortIntoAggBucket(bucketSortCollations, bucketSortFieldNames);
      }
      if (measureSortCollations != null) {
        temp.rePushDownSortAggMeasure(measureSortCollations, measureSortFieldNames);
      }
      if (rareTopDigest != null) {
        temp.rePushDownRareTop(rareTopDigest);
      }
      if (bucketSize != null) {
        temp.pushDownLimitIntoBucketSize(bucketSize);
      }
      return temp.getBuilderAndParser();
    } catch (AggregateAnalyzer.ExpressionNotAnalyzableException e) {
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot materialize aggregation pushdown", e);
    }
  }

  private static AggKind inferKind(@Nullable AggregationBuilder rootBuilder) {
    AggregationBuilder builder = unwrapNestedBuilder(rootBuilder);
    if (builder instanceof CompositeAggregationBuilder) {
      return AggKind.COMPOSITE;
    }
    if (builder instanceof TermsAggregationBuilder) {
      return AggKind.TERMS;
    }
    if (builder instanceof MultiTermsAggregationBuilder) {
      return AggKind.MULTI_TERMS;
    }
    if (builder
        instanceof
        org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder) {
      return AggKind.DATE_HISTOGRAM;
    }
    if (builder
        instanceof
        org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder) {
      return AggKind.HISTOGRAM;
    }
    if (builder instanceof TopHitsAggregationBuilder) {
      return AggKind.TOP_HITS;
    }
    return AggKind.OTHER;
  }

  private static boolean isRareTopSupported(@Nullable AggregationBuilder rootBuilder) {
    AggregationBuilder builder = unwrapNestedBuilder(rootBuilder);
    if (!(builder instanceof CompositeAggregationBuilder composite)) {
      return false;
    }
    if (composite.sources().size() == 1) {
      return composite.sources().getFirst() instanceof TermsValuesSourceBuilder terms
          && !terms.missingBucket();
    }
    return composite.sources().stream()
        .allMatch(src -> src instanceof TermsValuesSourceBuilder terms && !terms.missingBucket());
  }

  @Nullable
  private static AggKind inferMeasureSortTarget(@Nullable AggregationBuilder rootBuilder) {
    AggregationBuilder builder = unwrapNestedBuilder(rootBuilder);
    if (!(builder instanceof CompositeAggregationBuilder composite)) {
      return null;
    }
    if (composite.getSubAggregations().stream()
        .anyMatch(metric -> !(metric instanceof ValuesSourceAggregationBuilder.LeafOnly<?, ?>))) {
      return null;
    }
    if (composite.sources().size() == 1) {
      CompositeValuesSourceBuilder<?> source = composite.sources().getFirst();
      if (source instanceof TermsValuesSourceBuilder terms && !terms.missingBucket()) {
        return AggKind.TERMS;
      }
      if (source instanceof DateHistogramValuesSourceBuilder) {
        return AggKind.DATE_HISTOGRAM;
      }
      if (source instanceof HistogramValuesSourceBuilder histo && !histo.missingBucket()) {
        return AggKind.HISTOGRAM;
      }
      return null;
    }
    return composite.sources().stream()
            .allMatch(
                src -> src instanceof TermsValuesSourceBuilder terms && !terms.missingBucket())
        ? AggKind.MULTI_TERMS
        : null;
  }

  @Nullable
  private static Integer inferBucketSize(@Nullable AggregationBuilder rootBuilder) {
    AggregationBuilder builder = unwrapNestedBuilder(rootBuilder);
    if (builder instanceof CompositeAggregationBuilder composite) {
      return composite.size();
    }
    if (builder instanceof TermsAggregationBuilder terms) {
      return terms.size();
    }
    if (builder instanceof MultiTermsAggregationBuilder multiTerms) {
      return multiTerms.size();
    }
    if (builder instanceof TopHitsAggregationBuilder topHits) {
      return topHits.size();
    }
    return null;
  }

  @Nullable
  private static AggregationBuilder unwrapNestedBuilder(@Nullable AggregationBuilder rootBuilder) {
    if (rootBuilder instanceof NestedAggregationBuilder nested
        && !nested.getSubAggregations().isEmpty()) {
      return nested.getSubAggregations().iterator().next();
    }
    return rootBuilder;
  }
}

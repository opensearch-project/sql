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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/** Immutable aggregation pushdown state and ordered replay plan. */
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

  private enum LimitPushdownMode {
    UNSUPPORTED,
    ESTIMATE_ONLY,
    LEAF_METRIC,
    BUCKET_SIZE
  }

  private interface BuildAction extends AbstractAction<AggPushDownAction> {
    @Override
    default void pushOperation(PushDownContext context, PushDownOperation operation) {
      throw new UnsupportedOperationException("Internal aggregation build action cannot be queued");
    }
  }

  private final Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser>
      baseBuilderAndParser;
  private final Map<String, OpenSearchDataType> extendedTypeMapping;
  private final List<String> initialBucketNames;
  // Cost model uses the script count of the base logical aggregation. Supported rewrites keep the
  // same scripted sources/metrics semantically, while replay-time builders are request-scoped and
  // may not preserve a structure that can be re-counted accurately after rewrite.
  private final long scriptCount;
  private final AggKind kind;
  private final LimitPushdownMode limitPushdownMode;
  // The pushdown operation queue to rewrite base agg
  private final List<PushDownOperation> operationsForAgg;
  @Nullable private final Integer bucketSize;

  private AggSpec(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> baseBuilderAndParser,
      Map<String, OpenSearchDataType> extendedTypeMapping,
      List<String> initialBucketNames,
      long scriptCount,
      AggKind kind,
      LimitPushdownMode limitPushdownMode,
      List<PushDownOperation> operationsForAgg,
      @Nullable Integer bucketSize) {
    this.baseBuilderAndParser = baseBuilderAndParser;
    this.extendedTypeMapping = Map.copyOf(extendedTypeMapping);
    this.initialBucketNames = List.copyOf(initialBucketNames);
    this.scriptCount = scriptCount;
    this.kind = kind;
    this.limitPushdownMode = limitPushdownMode;
    this.operationsForAgg = List.copyOf(operationsForAgg);
    this.bucketSize = bucketSize;
  }

  public static AggSpec create(
      Map<String, OpenSearchDataType> extendedTypeMapping,
      List<String> bucketNames,
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> builderAndParser) {
    AggregationBuilder rootBuilder =
        builderAndParser.getLeft().isEmpty() ? null : builderAndParser.getLeft().getFirst();
    AggKind kind = inferKind(rootBuilder);
    return new AggSpec(
        builderAndParser,
        extendedTypeMapping,
        bucketNames,
        builderAndParser.getLeft().stream().mapToInt(AggPushDownAction::getScriptCount).sum(),
        kind,
        inferBaseLimitPushdownMode(rootBuilder, kind),
        List.of(),
        inferBucketSize(rootBuilder));
  }

  public boolean isCompositeAggregation() {
    return kind == AggKind.COMPOSITE;
  }

  public boolean canPushDownLimitIntoBucketSize(int size) {
    return switch (limitPushdownMode) {
      case BUCKET_SIZE -> bucketSize != null && size < bucketSize;
      case LEAF_METRIC -> true;
      case ESTIMATE_ONLY -> false;
      case UNSUPPORTED ->
          throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
              "Cannot pushdown limit into aggregation bucket");
    };
  }

  public AggSpec withBucketSort(List<RelFieldCollation> collations, List<String> fieldNames) {
    if (kind != AggKind.COMPOSITE && kind != AggKind.TERMS) {
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot pushdown sort into aggregation bucket");
    }
    if (kind == AggKind.COMPOSITE) {
      for (RelFieldCollation collation : collations) {
        String bucketName = fieldNames.get(collation.getFieldIndex());
        if (!initialBucketNames.contains(bucketName)) {
          throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
              "Cannot pushdown sort into aggregation bucket");
        }
      }
    }
    return new AggSpec(
        baseBuilderAndParser,
        extendedTypeMapping,
        initialBucketNames,
        scriptCount,
        kind,
        limitPushdownMode,
        replaceOperations(
            PushDownType.SORT,
            collations,
            action -> action.pushDownSortIntoAggBucket(collations, fieldNames)),
        bucketSize);
  }

  public AggSpec withoutBucketSort() {
    if (operationsForAgg.stream().noneMatch(operation -> operation.type() == PushDownType.SORT)) {
      return this;
    }
    return new AggSpec(
        baseBuilderAndParser,
        extendedTypeMapping,
        initialBucketNames,
        scriptCount,
        kind,
        limitPushdownMode,
        removeOperations(PushDownType.SORT),
        bucketSize);
  }

  public AggSpec withSortMeasure(List<RelFieldCollation> collations, List<String> fieldNames) {
    AggKind rewriteTarget = inferMeasureSortTarget();
    if (rewriteTarget == null) {
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot pushdown sort aggregate measure");
    }
    Integer resizedBucketSize =
        switch (rewriteTarget) {
          case TERMS, MULTI_TERMS -> bucketSize;
          default -> null;
        };
    return new AggSpec(
        baseBuilderAndParser,
        extendedTypeMapping,
        initialBucketNames,
        scriptCount,
        rewriteTarget,
        inferLimitPushdownMode(rewriteTarget),
        replaceOperations(
            PushDownType.SORT_AGG_METRICS,
            collations,
            action -> action.rePushDownSortAggMeasure(collations, fieldNames)),
        resizedBucketSize);
  }

  public AggSpec withRareTop(RareTopDigest digest) {
    if (!supportsCurrentRareTop()) {
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException("Cannot pushdown " + digest);
    }
    return new AggSpec(
        baseBuilderAndParser,
        extendedTypeMapping,
        initialBucketNames,
        scriptCount,
        AggKind.RARE_TOP,
        inferLimitPushdownMode(AggKind.RARE_TOP),
        replaceOperations(
            PushDownType.RARE_TOP, digest, action -> action.rePushDownRareTop(digest)),
        digest.byList().isEmpty() ? digest.number() : DEFAULT_MAX_BUCKETS);
  }

  public AggSpec withLimit(int size) {
    switch (limitPushdownMode) {
      case ESTIMATE_ONLY, LEAF_METRIC:
        return this;
      case UNSUPPORTED:
        throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
            "Cannot pushdown limit into aggregation bucket");
      case BUCKET_SIZE:
        if (!canPushDownLimitIntoBucketSize(size)) {
          return this;
        }
        break;
    }
    return new AggSpec(
        baseBuilderAndParser,
        extendedTypeMapping,
        initialBucketNames,
        scriptCount,
        kind,
        limitPushdownMode,
        replaceOperations(
            PushDownType.LIMIT,
            new LimitDigest(size, 0),
            action -> action.pushDownLimitIntoBucketSize(size)),
        size);
  }

  public AggPushDownAction buildAction() {
    AggPushDownAction action =
        new AggPushDownAction(
            baseBuilderAndParser, extendedTypeMapping, new ArrayList<>(initialBucketNames));
    operationsForAgg.forEach(operation -> ((BuildAction) operation.action()).apply(action));
    return action;
  }

  private List<PushDownOperation> replaceOperations(
      PushDownType type, Object digest, BuildAction action) {
    List<PushDownOperation> newOperations = removeOperations(type);
    newOperations.add(new PushDownOperation(type, digest, action));
    return newOperations;
  }

  private List<PushDownOperation> removeOperations(PushDownType type) {
    return new ArrayList<>(
        operationsForAgg.stream().filter(operation -> operation.type() != type).toList());
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
    if (builder instanceof DateHistogramAggregationBuilder) {
      return AggKind.DATE_HISTOGRAM;
    }
    if (builder instanceof HistogramAggregationBuilder) {
      return AggKind.HISTOGRAM;
    }
    if (builder instanceof TopHitsAggregationBuilder) {
      return AggKind.TOP_HITS;
    }
    return AggKind.OTHER;
  }

  private static LimitPushdownMode inferLimitPushdownMode(AggKind kind) {
    return switch (kind) {
      case COMPOSITE, TERMS, MULTI_TERMS, TOP_HITS, RARE_TOP -> LimitPushdownMode.BUCKET_SIZE;
      case OTHER, DATE_HISTOGRAM, HISTOGRAM -> LimitPushdownMode.UNSUPPORTED;
    };
  }

  private static LimitPushdownMode inferBaseLimitPushdownMode(
      @Nullable AggregationBuilder rootBuilder, AggKind kind) {
    if (rootBuilder == null) {
      // count() optimization uses hits.total and leaves the builder list empty. Keeps
      // LIMIT in PushDownContext for these cases even though no request-side limit is applied.
      return LimitPushdownMode.ESTIMATE_ONLY;
    }
    AggregationBuilder builder = unwrapNestedBuilder(rootBuilder);
    if (builder instanceof ValuesSourceAggregationBuilder.LeafOnly<?, ?>) {
      // Treats leaf metric aggregations as limit-pushable because they produce a single row.
      return LimitPushdownMode.LEAF_METRIC;
    }
    return inferLimitPushdownMode(kind);
  }

  private static boolean supportsBaseRareTop(@Nullable AggregationBuilder rootBuilder) {
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
  private AggKind inferMeasureSortTarget() {
    if (kind != AggKind.COMPOSITE) {
      return null;
    }
    AggregationBuilder rootBuilder =
        baseBuilderAndParser.getLeft().isEmpty() ? null : baseBuilderAndParser.getLeft().getFirst();
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

  private boolean supportsCurrentRareTop() {
    return kind == AggKind.COMPOSITE
        && supportsBaseRareTop(
            baseBuilderAndParser.getLeft().isEmpty()
                ? null
                : baseBuilderAndParser.getLeft().getFirst());
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

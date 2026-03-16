/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import static org.opensearch.search.aggregations.MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.BucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.MetricParserHelper;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

/** A lambda aggregation pushdown action to apply on the {@link OpenSearchRequestBuilder} */
@Getter
@EqualsAndHashCode
public class AggPushDownAction implements OSRequestBuilderAction {
  private Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> builderAndParser;
  private final Map<String, OpenSearchDataType> extendedTypeMapping;
  private final long scriptCount;
  // Record the output field names of all buckets as the sequence of buckets
  private List<String> bucketNames;

  public AggPushDownAction(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> builderAndParser,
      Map<String, OpenSearchDataType> extendedTypeMapping,
      List<String> bucketNames) {
    this.builderAndParser = builderAndParser;
    this.extendedTypeMapping = extendedTypeMapping;
    this.scriptCount =
        builderAndParser.getLeft().stream().mapToInt(AggPushDownAction::getScriptCount).sum();
    this.bucketNames = bucketNames;
  }

  private static AggregatorFactories.Builder copySubAggregations(AggregationBuilder source) {
    AggregatorFactories.Builder copiedFactories = new AggregatorFactories.Builder();
    source.getSubAggregations().forEach(copiedFactories::addAggregator);
    source.getPipelineAggregations().forEach(copiedFactories::addPipelineAggregator);
    return copiedFactories;
  }

  private static AggregationBuilder unwrapNestedBuilder(AggregationBuilder rootBuilder) {
    if (rootBuilder instanceof NestedAggregationBuilder nested
        && !nested.getSubAggregations().isEmpty()) {
      return nested.getSubAggregations().iterator().next();
    }
    return rootBuilder;
  }

  private void replaceRootBuilder(
      AggregationBuilder originalRoot, AggregationBuilder newInnerBuilder) {
    AggregationBuilder finalBuilder = newInnerBuilder;
    if (originalRoot instanceof NestedAggregationBuilder nested) {
      finalBuilder =
          AggregationBuilders.nested(nested.getName(), nested.path())
              .subAggregation(newInnerBuilder);
    }
    builderAndParser =
        Pair.of(Collections.singletonList(finalBuilder), builderAndParser.getRight());
  }

  private static int getScriptCount(AggregationBuilder aggBuilder) {
    if (aggBuilder instanceof NestedAggregationBuilder) {
      aggBuilder = aggBuilder.getSubAggregations().iterator().next();
    }
    if (aggBuilder instanceof ValuesSourceAggregationBuilder<?>
        && ((ValuesSourceAggregationBuilder<?>) aggBuilder).script() != null) return 1;
    if (aggBuilder instanceof CompositeAggregationBuilder) {
      CompositeAggregationBuilder compositeAggBuilder = (CompositeAggregationBuilder) aggBuilder;
      int sourceScriptCount =
          compositeAggBuilder.sources().stream()
              .mapToInt(source -> source.script() != null ? 1 : 0)
              .sum();
      int subAggScriptCount =
          compositeAggBuilder.getSubAggregations().stream()
              .mapToInt(AggPushDownAction::getScriptCount)
              .sum();
      return sourceScriptCount + subAggScriptCount;
    }
    return 0;
  }

  @Override
  public void apply(OpenSearchRequestBuilder requestBuilder) {
    requestBuilder.pushDownAggregation(builderAndParser);
    requestBuilder.pushTypeMapping(extendedTypeMapping);
  }

  /** Convert a {@link CompositeAggregationParser} to {@link BucketAggregationParser} */
  private BucketAggregationParser convertTo(OpenSearchAggregationResponseParser parser) {
    if (parser instanceof BucketAggregationParser) {
      return (BucketAggregationParser) parser;
    } else if (parser instanceof CompositeAggregationParser) {
      MetricParserHelper helper = ((CompositeAggregationParser) parser).getMetricsParser();
      return new BucketAggregationParser(
          helper.getMetricParserMap().values().stream().toList(), helper.getCountAggNameList());
    } else {
      throw new IllegalStateException("Unexpected parser type: " + parser.getClass());
    }
  }

  private String multiTermsBucketNameAsString(CompositeAggregationBuilder composite) {
    return composite.sources().stream()
        .map(TermsValuesSourceBuilder.class::cast)
        .map(TermsValuesSourceBuilder::name)
        .reduce((left, right) -> left + "|" + right)
        .orElse(""); // PIPE cannot be used in identifier
  }

  /** Re-pushdown a sort aggregation measure to replace the pushed composite aggregation */
  public void rePushDownSortAggMeasure(
      List<RelFieldCollation> collations, List<String> fieldNames) {
    if (builderAndParser.getLeft().isEmpty()) return;
    AggregationBuilder original = builderAndParser.getLeft().getFirst();
    AggregationBuilder builder;
    if (original instanceof NestedAggregationBuilder) {
      builder = original.getSubAggregations().iterator().next();
    } else {
      builder = original;
    }
    if (builder instanceof CompositeAggregationBuilder composite) {
      boolean asc = collations.get(0).getDirection() == RelFieldCollation.Direction.ASCENDING;
      String path = getAggregationPath(collations, fieldNames, composite);
      BucketOrder bucketOrder =
          composite.getSubAggregations().isEmpty()
              ? BucketOrder.count(asc)
              : BucketOrder.aggregation(path, asc);
      AggregationBuilder aggregationBuilder = null;
      if (composite.sources().size() == 1) {
        if (composite.sources().get(0) instanceof TermsValuesSourceBuilder terms
            && !terms.missingBucket()) {
          aggregationBuilder = buildTermsAggregationBuilder(terms, bucketOrder, composite.size());
          attachSubAggregations(composite.getSubAggregations(), path, aggregationBuilder);
        } else if (composite.sources().get(0)
            instanceof DateHistogramValuesSourceBuilder dateHisto) {
          aggregationBuilder = buildDateHistogramAggregationBuilder(dateHisto, bucketOrder);
          attachSubAggregations(composite.getSubAggregations(), path, aggregationBuilder);
        } else if (composite.sources().get(0) instanceof HistogramValuesSourceBuilder histo
            && !histo.missingBucket()) {
          aggregationBuilder = buildHistogramAggregationBuilder(histo, bucketOrder);
          attachSubAggregations(composite.getSubAggregations(), path, aggregationBuilder);
        } else {
          throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
              "Cannot pushdown sort aggregate measure");
        }
      } else {
        if (composite.sources().stream()
            .allMatch(
                src -> src instanceof TermsValuesSourceBuilder terms && !terms.missingBucket())) {
          // multi-term agg
          aggregationBuilder = buildMultiTermsAggregationBuilder(composite, bucketOrder);
          attachSubAggregations(composite.getSubAggregations(), path, aggregationBuilder);
        } else {
          throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
              "Cannot pushdown sort aggregate measure");
        }
      }
      if (original instanceof NestedAggregationBuilder nested) {
        aggregationBuilder =
            AggregationBuilders.nested(nested.getName(), nested.path())
                .subAggregation(aggregationBuilder);
      }
      builderAndParser =
          Pair.of(
              Collections.singletonList(aggregationBuilder),
              convertTo(builderAndParser.getRight()));
    }
  }

  /** Re-pushdown a nested aggregation for rare/top to replace the pushed composite aggregation */
  public void rePushDownRareTop(RareTopDigest digest) {
    if (builderAndParser.getLeft().isEmpty()) return;
    AggregationBuilder original = builderAndParser.getLeft().getFirst();
    AggregationBuilder builder;
    if (original instanceof NestedAggregationBuilder) {
      builder = original.getSubAggregations().iterator().next();
    } else {
      builder = original;
    }
    if (builder instanceof CompositeAggregationBuilder composite) {
      BucketOrder bucketOrder =
          digest.direction() == RelFieldCollation.Direction.ASCENDING
              ? BucketOrder.count(true)
              : BucketOrder.count(false);
      AggregationBuilder aggregationBuilder = null;
      if (composite.sources().size() == 1) {
        if (composite.sources().get(0) instanceof TermsValuesSourceBuilder terms
            && !terms.missingBucket()) {
          aggregationBuilder = buildTermsAggregationBuilder(terms, bucketOrder, digest.number());
        } else if (composite.sources().get(0)
            instanceof DateHistogramValuesSourceBuilder dateHisto) {
          // for top/rare, only field can be used in by-clause, so this branch never accessed now
          aggregationBuilder = buildDateHistogramAggregationBuilder(dateHisto, bucketOrder);
        } else if (composite.sources().get(0) instanceof HistogramValuesSourceBuilder histo
            && !histo.missingBucket()) {
          // for top/rare, only field can be used in by-clause, so this branch never accessed now
          aggregationBuilder = buildHistogramAggregationBuilder(histo, bucketOrder);
        } else {
          throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
              "Cannot pushdown " + digest);
        }
      } else {
        if (composite.sources().stream()
            .allMatch(
                src -> src instanceof TermsValuesSourceBuilder terms && !terms.missingBucket())) {
          for (int i = 0; i < composite.sources().size(); i++) {
            TermsValuesSourceBuilder terms = (TermsValuesSourceBuilder) composite.sources().get(i);
            if (i == 0) { // first
              aggregationBuilder = buildTermsAggregationBuilder(terms, null, DEFAULT_MAX_BUCKETS);
            } else if (i == composite.sources().size() - 1) { // last
              aggregationBuilder.subAggregation(
                  buildTermsAggregationBuilder(terms, bucketOrder, digest.number()));
            } else {
              aggregationBuilder.subAggregation(
                  buildTermsAggregationBuilder(terms, null, DEFAULT_MAX_BUCKETS));
            }
          }
        } else {
          throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
              "Cannot pushdown " + digest);
        }
      }
      if (aggregationBuilder != null && original instanceof NestedAggregationBuilder nested) {
        aggregationBuilder =
            AggregationBuilders.nested(nested.getName(), nested.path())
                .subAggregation(aggregationBuilder);
      }
      builderAndParser =
          Pair.of(
              Collections.singletonList(aggregationBuilder),
              convertTo(builderAndParser.getRight()));
    }
  }

  /** Build a {@link TermsAggregationBuilder} by {@link TermsValuesSourceBuilder} */
  private TermsAggregationBuilder buildTermsAggregationBuilder(
      TermsValuesSourceBuilder terms, BucketOrder bucketOrder, int newSize) {
    TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder(terms.name());
    termsBuilder.size(newSize);
    if (terms.field() != null) {
      termsBuilder.field(terms.field());
    }
    if (terms.script() != null) {
      termsBuilder.script(terms.script());
    }
    if (terms.userValuetypeHint() != null) {
      termsBuilder.userValueTypeHint(terms.userValuetypeHint());
    }
    if (bucketOrder != null) {
      termsBuilder.order(bucketOrder);
    }
    return termsBuilder;
  }

  private static void copyDateHistogramInterval(
      DateHistogramValuesSourceBuilder source,
      Consumer<DateHistogramInterval> fixedIntervalSetter,
      Consumer<DateHistogramInterval> calendarIntervalSetter) {
    try {
      fixedIntervalSetter.accept(source.getIntervalAsFixed());
      return;
    } catch (IllegalArgumentException | IllegalStateException ignored) {
      // Fallback to calendar interval.
    }
    try {
      calendarIntervalSetter.accept(source.getIntervalAsCalendar());
      return;
    } catch (IllegalArgumentException | IllegalStateException ignored) {
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot copy interval for date histogram bucket " + source.name());
    }
  }

  private static void copyDateHistogramBucketOptions(
      DateHistogramValuesSourceBuilder source, DateHistogramAggregationBuilder target) {
    if (source.field() != null) {
      target.field(source.field());
    }
    if (source.script() != null) {
      target.script(source.script());
    }
    copyDateHistogramInterval(source, target::fixedInterval, target::calendarInterval);
    if (source.userValuetypeHint() != null) {
      target.userValueTypeHint(source.userValuetypeHint());
    }
    if (source.timeZone() != null) {
      target.timeZone(source.timeZone());
    }
    if (source.offset() != 0) {
      target.offset(source.offset());
    }
    if (source.format() != null) {
      target.format(source.format());
    }
    // Composite group-by only returns buckets with documents. Preserve that when rewriting.
    target.minDocCount(1);
  }

  private static void copyHistogramBucketOptions(
      HistogramValuesSourceBuilder source, HistogramAggregationBuilder target) {
    if (source.field() != null) {
      target.field(source.field());
    }
    if (source.script() != null) {
      target.script(source.script());
    }
    target.interval(source.interval());
    if (source.userValuetypeHint() != null) {
      target.userValueTypeHint(source.userValuetypeHint());
    }
    if (source.format() != null) {
      target.format(source.format());
    }
    // Composite group-by only returns buckets with documents. Preserve that when rewriting.
    target.minDocCount(1);
  }

  /** Build a {@link DateHistogramAggregationBuilder} by {@link DateHistogramValuesSourceBuilder} */
  private DateHistogramAggregationBuilder buildDateHistogramAggregationBuilder(
      DateHistogramValuesSourceBuilder dateHisto, BucketOrder bucketOrder) {
    DateHistogramAggregationBuilder dateHistoBuilder =
        new DateHistogramAggregationBuilder(dateHisto.name());
    copyDateHistogramBucketOptions(dateHisto, dateHistoBuilder);
    dateHistoBuilder.order(bucketOrder);
    return dateHistoBuilder;
  }

  /** Build a {@link HistogramAggregationBuilder} by {@link HistogramValuesSourceBuilder} */
  private HistogramAggregationBuilder buildHistogramAggregationBuilder(
      HistogramValuesSourceBuilder histo, BucketOrder bucketOrder) {
    HistogramAggregationBuilder histoBuilder = new HistogramAggregationBuilder(histo.name());
    copyHistogramBucketOptions(histo, histoBuilder);
    histoBuilder.order(bucketOrder);
    return histoBuilder;
  }

  /** Build a {@link MultiTermsAggregationBuilder} by {@link CompositeAggregationBuilder} */
  private MultiTermsAggregationBuilder buildMultiTermsAggregationBuilder(
      CompositeAggregationBuilder composite, BucketOrder bucketOrder) {
    MultiTermsAggregationBuilder multiTermsBuilder =
        new MultiTermsAggregationBuilder(multiTermsBucketNameAsString(composite));
    multiTermsBuilder.size(composite.size());
    multiTermsBuilder.terms(
        composite.sources().stream()
            .map(TermsValuesSourceBuilder.class::cast)
            .map(
                termValue -> {
                  MultiTermsValuesSourceConfig.Builder config =
                      new MultiTermsValuesSourceConfig.Builder();
                  config.setFieldName(termValue.field());
                  if (termValue.script() != null) {
                    config.setScript(termValue.script());
                  }
                  config.setUserValueTypeHint(termValue.userValuetypeHint());
                  return config.build();
                })
            .toList());
    multiTermsBuilder.order(bucketOrder);
    return multiTermsBuilder;
  }

  private String getAggregationPath(
      List<RelFieldCollation> collations,
      List<String> fieldNames,
      CompositeAggregationBuilder composite) {
    AggregationBuilder metric = composite.getSubAggregations().stream().findFirst().orElse(null);
    if (metric != null && !(metric instanceof ValuesSourceAggregationBuilder.LeafOnly)) {
      // do not pushdown sort aggregate measure for nested aggregation, e.g. composite then range
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot pushdown sort aggregate measure, composite.getSubAggregations() is not a"
              + " LeafOnly");
    }
    return fieldNames.get(collations.get(0).getFieldIndex());
  }

  private AggregationBuilder attachSubAggregations(
      Collection<AggregationBuilder> subAggregations,
      String path,
      AggregationBuilder aggregationBuilder) {
    if (!subAggregations.isEmpty()) {
      AggregatorFactories.Builder metricBuilder = new AggregatorFactories.Builder();
      subAggregations.forEach(metricBuilder::addAggregator);
      // the count aggregator may be eliminated by doc_count optimization, add it back
      if (subAggregations.stream().noneMatch(sub -> sub.getName().equals(path))) {
        metricBuilder.addAggregator(AggregationBuilders.count(path).field("_index"));
      }
      aggregationBuilder.subAggregations(metricBuilder);
    }
    return aggregationBuilder;
  }

  public void pushDownSortIntoAggBucket(
      List<RelFieldCollation> collations, List<String> fieldNames) {
    // aggregationBuilder.getLeft() could be empty when count agg optimization works
    if (builderAndParser.getLeft().isEmpty()) return;
    AggregationBuilder original = builderAndParser.getLeft().getFirst();
    AggregationBuilder builder = unwrapNestedBuilder(original);
    if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
      pushDownSortIntoCompositeBucket(original, compositeAggBuilder, collations, fieldNames);
      return;
    }
    if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
      pushDownSortIntoTermsBucket(original, termsAggBuilder, collations);
    }
    // TODO for MultiTermsAggregationBuilder
  }

  private void pushDownSortIntoCompositeBucket(
      AggregationBuilder original,
      CompositeAggregationBuilder compositeAggBuilder,
      List<RelFieldCollation> collations,
      List<String> fieldNames) {
    // It will always use a single CompositeAggregationBuilder for the aggregation with GroupBy.
    // See {@link AggregateAnalyzer}
    List<CompositeValuesSourceBuilder<?>> buckets = compositeAggBuilder.sources();
    List<CompositeValuesSourceBuilder<?>> newBuckets = new ArrayList<>(buckets.size());
    List<String> newBucketNames = new ArrayList<>(buckets.size());
    List<String> selected = new ArrayList<>(collations.size());

    // Have to put the collation required buckets first, then the rest of buckets.
    collations.forEach(
        collation -> {
          /*
           Must find the bucket by field name because:
             1. The sequence of buckets may have changed after sort push-down.
             2. The schema of scan operator may be inconsistent with the sequence of buckets
             after project push-down.
          */
          String bucketName = fieldNames.get(collation.getFieldIndex());
          CompositeValuesSourceBuilder<?> bucket = buckets.get(bucketNames.indexOf(bucketName));
          RelFieldCollation.Direction direction = collation.getDirection();
          RelFieldCollation.NullDirection nullDirection = collation.nullDirection;
          SortOrder order =
              RelFieldCollation.Direction.DESCENDING.equals(direction)
                  ? SortOrder.DESC
                  : SortOrder.ASC;
          if (bucket.missingBucket()) {
            MissingOrder missingOrder =
                switch (nullDirection) {
                  case FIRST -> MissingOrder.FIRST;
                  case LAST -> MissingOrder.LAST;
                  default -> MissingOrder.DEFAULT;
                };
            bucket.missingOrder(missingOrder);
          }
          newBuckets.add(bucket.order(order));
          newBucketNames.add(bucketName);
          selected.add(bucketName);
        });

    buckets.stream()
        .map(CompositeValuesSourceBuilder::name)
        .filter(name -> !selected.contains(name))
        .forEach(
            name -> {
              newBuckets.add(buckets.get(bucketNames.indexOf(name)));
              newBucketNames.add(name);
            });

    AggregationBuilder finalBuilder =
        AggregationBuilders.composite(compositeAggBuilder.getName(), newBuckets)
            .subAggregations(copySubAggregations(compositeAggBuilder))
            .size(compositeAggBuilder.size());
    replaceRootBuilder(original, finalBuilder);
    bucketNames = newBucketNames;
  }

  private void pushDownSortIntoTermsBucket(
      AggregationBuilder original,
      TermsAggregationBuilder termsAggBuilder,
      List<RelFieldCollation> collations) {
    termsAggBuilder.order(BucketOrder.key(!collations.getFirst().getDirection().isDescending()));
  }

  private static Integer getBucketSize(AggregationBuilder builder) {
    if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
      return compositeAggBuilder.size();
    }
    if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
      return termsAggBuilder.size();
    }
    if (builder instanceof MultiTermsAggregationBuilder multiTermsAggBuilder) {
      return multiTermsAggBuilder.size();
    }
    if (builder
        instanceof
        org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder topHitsAggBuilder) {
      return topHitsAggBuilder.size();
    }
    return null;
  }

  private static void resizeBucketBuilder(AggregationBuilder builder, int size) {
    if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
      compositeAggBuilder.size(size);
      return;
    }
    if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
      termsAggBuilder.size(size);
      return;
    }
    if (builder instanceof MultiTermsAggregationBuilder multiTermsAggBuilder) {
      multiTermsAggBuilder.size(size);
      return;
    }
    if (builder
        instanceof
        org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder topHitsAggBuilder) {
      topHitsAggBuilder.size(size);
      return;
    }
    throw new IllegalStateException(
        "Not a resizable bucket aggregation builder: " + builder.getClass().getSimpleName());
  }

  private AggregationBuilder resizeAggregationForLimit(AggregationBuilder builder, int size) {
    Integer bucketSize = getBucketSize(builder);
    if (bucketSize != null) {
      if (size < bucketSize) {
        resizeBucketBuilder(builder, size);
        return builder;
      }
      return null;
    }
    if (builder instanceof ValuesSourceAggregationBuilder.LeafOnly<?, ?>) {
      // all metric aggregations generate one row and are effectively already limited.
      return builder;
    }
    throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
        "Unknown aggregation builder " + builder.getClass().getSimpleName());
  }

  /**
   * Check if the limit can be pushed down into aggregation bucket when the limit size is less than
   * bucket number.
   */
  public boolean pushDownLimitIntoBucketSize(Integer size) {
    // aggregationBuilder.getLeft() could be empty when count agg optimization works
    if (builderAndParser.getLeft().isEmpty()) return false;
    AggregationBuilder original = builderAndParser.getLeft().getFirst();
    AggregationBuilder builder = unwrapNestedBuilder(original);
    AggregationBuilder resizedBuilder = resizeAggregationForLimit(builder, size);
    if (resizedBuilder == null) {
      return false;
    }
    if (resizedBuilder != builder) {
      replaceRootBuilder(original, resizedBuilder);
    }
    return true;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
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
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
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

  private Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder;
  private final Map<String, OpenSearchDataType> extendedTypeMapping;
  private final long scriptCount;
  // Record the output field names of all buckets as the sequence of buckets
  private List<String> bucketNames;

  public AggPushDownAction(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder,
      Map<String, OpenSearchDataType> extendedTypeMapping,
      List<String> bucketNames) {
    this.aggregationBuilder = aggregationBuilder;
    this.extendedTypeMapping = extendedTypeMapping;
    this.scriptCount =
        aggregationBuilder.getLeft().stream().filter(this::isScriptAggBuilder).count();
    this.bucketNames = bucketNames;
  }

  private boolean isScriptAggBuilder(AggregationBuilder aggBuilder) {
    return aggBuilder instanceof ValuesSourceAggregationBuilder<?> valueSourceAgg
        && valueSourceAgg.script() != null;
  }

  @Override
  public void apply(OpenSearchRequestBuilder requestBuilder) {
    requestBuilder.pushDownAggregation(aggregationBuilder);
    requestBuilder.pushTypeMapping(extendedTypeMapping);
  }

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
        .collect(Collectors.joining("|")); // PIPE cannot be used in identifier
  }

  public void pushDownSortAggMetrics(List<RelFieldCollation> collations, List<String> fieldNames) {
    if (aggregationBuilder.getLeft().isEmpty()) return;
    AggregationBuilder builder = aggregationBuilder.getLeft().getFirst();
    if (builder instanceof CompositeAggregationBuilder composite) {
      String path = getAggregationPath(collations, fieldNames, composite);
      BucketOrder bucketOrder =
          collations.get(0).getDirection() == RelFieldCollation.Direction.ASCENDING
              ? BucketOrder.aggregation(path, true)
              : BucketOrder.aggregation(path, false);

      if (composite.sources().size() == 1) {
        if (composite.sources().get(0) instanceof TermsValuesSourceBuilder terms
            && !terms.missingBucket()) {
          TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder(terms.name());
          termsBuilder.size(composite.size());
          termsBuilder.field(terms.field());
          if (terms.userValuetypeHint() != null) {
            termsBuilder.userValueTypeHint(terms.userValuetypeHint());
          }
          termsBuilder.order(bucketOrder);
          attachSubAggregations(composite.getSubAggregations(), path, termsBuilder);
          aggregationBuilder =
              Pair.of(
                  Collections.singletonList(termsBuilder),
                  convertTo(aggregationBuilder.getRight()));
          return;
        } else if (composite.sources().get(0)
            instanceof DateHistogramValuesSourceBuilder dateHisto) {
          DateHistogramAggregationBuilder dateHistoBuilder =
              new DateHistogramAggregationBuilder(dateHisto.name());
          dateHistoBuilder.field(dateHisto.field());
          try {
            dateHistoBuilder.fixedInterval(dateHisto.getIntervalAsFixed());
          } catch (IllegalArgumentException e) {
            dateHistoBuilder.calendarInterval(dateHisto.getIntervalAsCalendar());
          }
          if (dateHisto.userValuetypeHint() != null) {
            dateHistoBuilder.userValueTypeHint(dateHisto.userValuetypeHint());
          }
          dateHistoBuilder.order(bucketOrder);
          attachSubAggregations(composite.getSubAggregations(), path, dateHistoBuilder);
          aggregationBuilder =
              Pair.of(
                  Collections.singletonList(dateHistoBuilder),
                  convertTo(aggregationBuilder.getRight()));
          return;
        } else if (composite.sources().get(0) instanceof HistogramValuesSourceBuilder histo
            && !histo.missingBucket()) {
          HistogramAggregationBuilder histoBuilder = new HistogramAggregationBuilder(histo.name());
          histoBuilder.field(histo.field());
          histoBuilder.interval(histo.interval());
          if (histo.userValuetypeHint() != null) {
            histoBuilder.userValueTypeHint(histo.userValuetypeHint());
          }
          histoBuilder.order(bucketOrder);
          attachSubAggregations(composite.getSubAggregations(), path, histoBuilder);
          aggregationBuilder =
              Pair.of(
                  Collections.singletonList(histoBuilder),
                  convertTo(aggregationBuilder.getRight()));
          return;
        }
      } else {
        if (composite.sources().stream()
            .allMatch(
                src -> src instanceof TermsValuesSourceBuilder terms && !terms.missingBucket())) {
          // multi-term agg
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
                        config.setUserValueTypeHint(termValue.userValuetypeHint());
                        return config.build();
                      })
                  .toList());
          attachSubAggregations(composite.getSubAggregations(), path, multiTermsBuilder);
          aggregationBuilder =
              Pair.of(
                  Collections.singletonList(multiTermsBuilder),
                  convertTo(aggregationBuilder.getRight()));
          return;
        }
      }
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot pushdown sort aggregate metrics");
    }
  }

  private String getAggregationPath(
      List<RelFieldCollation> collations,
      List<String> fieldNames,
      CompositeAggregationBuilder composite) {
    String path;
    AggregationBuilder metric = composite.getSubAggregations().stream().findFirst().orElse(null);
    if (metric == null) {
      // count agg optimized, get the path name from field names
      path = fieldNames.get(collations.get(0).getFieldIndex());
    } else if (metric instanceof ValuesSourceAggregationBuilder.LeafOnly) {
      path = metric.getName();
    } else {
      // we do not support pushdown sort aggregate metrics for nested aggregation
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Cannot pushdown sort aggregate metrics, composite.getSubAggregations() is not a"
              + " LeafOnly");
    }
    return path;
  }

  private <T extends AbstractAggregationBuilder<T>> T attachSubAggregations(
      Collection<AggregationBuilder> subAggregations, String path, T aggregationBuilder) {
    AggregatorFactories.Builder metricBuilder = new AggregatorFactories.Builder();
    if (subAggregations.isEmpty()) {
      metricBuilder.addAggregator(AggregationBuilders.count(path).field("_index"));
    } else {
      metricBuilder.addAggregator(subAggregations.stream().toList().get(0));
    }
    aggregationBuilder.subAggregations(metricBuilder);
    return aggregationBuilder;
  }

  public void pushDownSortIntoAggBucket(
      List<RelFieldCollation> collations, List<String> fieldNames) {
    // aggregationBuilder.getLeft() could be empty when count agg optimization works
    if (aggregationBuilder.getLeft().isEmpty()) return;
    AggregationBuilder builder = aggregationBuilder.getLeft().getFirst();
    List<String> selected = new ArrayList<>(collations.size());
    if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
      // It will always use a single CompositeAggregationBuilder for the aggregation with GroupBy
      // See {@link AggregateAnalyzer}
      List<CompositeValuesSourceBuilder<?>> buckets = compositeAggBuilder.sources();
      List<CompositeValuesSourceBuilder<?>> newBuckets = new ArrayList<>(buckets.size());
      List<String> newBucketNames = new ArrayList<>(buckets.size());
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
      AggregatorFactories.Builder newAggBuilder = new AggregatorFactories.Builder();
      compositeAggBuilder.getSubAggregations().forEach(newAggBuilder::addAggregator);
      aggregationBuilder =
          Pair.of(
              Collections.singletonList(
                  AggregationBuilders.composite("composite_buckets", newBuckets)
                      .subAggregations(newAggBuilder)
                      .size(compositeAggBuilder.size())),
              aggregationBuilder.getRight());
      bucketNames = newBucketNames;
    }
    if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
      termsAggBuilder.order(BucketOrder.key(!collations.getFirst().getDirection().isDescending()));
    }
    // TODO for MultiTermsAggregationBuilder
  }

  /**
   * Check if the limit can be pushed down into aggregation bucket when the limit size is less than
   * bucket number.
   */
  public boolean pushDownLimitIntoBucketSize(Integer size) {
    // aggregationBuilder.getLeft() could be empty when count agg optimization works
    if (aggregationBuilder.getLeft().isEmpty()) return false;
    AggregationBuilder builder = aggregationBuilder.getLeft().getFirst();
    if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
      if (size < compositeAggBuilder.size()) {
        compositeAggBuilder.size(size);
        return true;
      } else {
        return false;
      }
    }
    if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
      if (size < termsAggBuilder.size()) {
        termsAggBuilder.size(size);
        return true;
      } else {
        return false;
      }
    }
    if (builder instanceof MultiTermsAggregationBuilder multiTermsAggBuilder) {
      if (size < multiTermsAggBuilder.size()) {
        multiTermsAggBuilder.size(size);
        return true;
      } else {
        return false;
      }
    }
    // now we only have Composite, Terms and MultiTerms bucket aggregations,
    // add code here when we could support more in the future.
    if (builder instanceof ValuesSourceAggregationBuilder.LeafOnly<?, ?>) {
      // Note: all metric aggregations will be treated as pushed since it generates only one row.
      return true;
    }
    throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
        "Unknown aggregation builder " + builder.getClass().getSimpleName());
  }
}

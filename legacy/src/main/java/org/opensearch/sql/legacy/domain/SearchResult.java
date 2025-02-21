/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.opensearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.opensearch.search.aggregations.metrics.InternalTopHits;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.sql.legacy.exception.SqlParseException;

public class SearchResult {
  /** */
  private List<Map<String, Object>> results;

  private long total;

  double maxScore = 0;

  public SearchResult(SearchResponse resp) {
    SearchHits hits = resp.getHits();
    this.total = Optional.ofNullable(hits.getTotalHits()).map(TotalHits::value).orElse(0L);
    results = new ArrayList<>(hits.getHits().length);
    for (SearchHit searchHit : hits.getHits()) {
      if (searchHit.getSourceAsMap() != null) {
        results.add(searchHit.getSourceAsMap());
      } else if (searchHit.getFields() != null) {
        Map<String, DocumentField> fields = searchHit.getFields();
        results.add(toFieldsMap(fields));
      }
    }
  }

  public SearchResult(SearchResponse resp, Select select) throws SqlParseException {
    Aggregations aggs = resp.getAggregations();
    if (aggs.get("filter") != null) {
      InternalFilter inf = aggs.get("filter");
      aggs = inf.getAggregations();
    }
    if (aggs.get("group by") != null) {
      InternalTerms terms = aggs.get("group by");
      Collection<Bucket> buckets = terms.getBuckets();
      this.total = buckets.size();
      results = new ArrayList<>(buckets.size());
      for (Bucket bucket : buckets) {
        Map<String, Object> aggsMap = toAggsMap(bucket.getAggregations().getAsMap());
        aggsMap.put("docCount", bucket.getDocCount());
        results.add(aggsMap);
      }
    } else {
      results = new ArrayList<>(1);
      this.total = 1;
      Map<String, Object> map = new HashMap<>();
      for (Aggregation aggregation : aggs) {
        map.put(aggregation.getName(), covenValue(aggregation));
      }
      results.add(map);
    }
  }

  /**
   * @param fields
   * @return
   */
  private Map<String, Object> toFieldsMap(Map<String, DocumentField> fields) {
    Map<String, Object> result = new HashMap<>();
    for (Entry<String, DocumentField> entry : fields.entrySet()) {
      if (entry.getValue().getValues().size() > 1) {
        result.put(entry.getKey(), entry.getValue().getValues());
      } else {
        result.put(entry.getKey(), entry.getValue().getValue());
      }
    }
    return result;
  }

  /**
   * @param fields
   * @return
   * @throws SqlParseException
   */
  private Map<String, Object> toAggsMap(Map<String, Aggregation> fields) throws SqlParseException {
    Map<String, Object> result = new HashMap<>();
    for (Entry<String, Aggregation> entry : fields.entrySet()) {
      result.put(entry.getKey(), covenValue(entry.getValue()));
    }
    return result;
  }

  private Object covenValue(Aggregation value) throws SqlParseException {
    if (value instanceof InternalNumericMetricsAggregation.SingleValue) {
      return ((InternalNumericMetricsAggregation.SingleValue) value).value();
    } else if (value instanceof InternalValueCount) {
      return ((InternalValueCount) value).getValue();
    } else if (value instanceof InternalTopHits) {
      return (value);
    } else if (value instanceof LongTerms) {
      return value;
    } else {
      throw new SqlParseException(
          "Unknown aggregation value type: " + value.getClass().getSimpleName());
    }
  }

  public List<Map<String, Object>> getResults() {
    return results;
  }

  public void setResults(List<Map<String, Object>> results) {
    this.results = results;
  }

  public long getTotal() {
    return total;
  }

  public void setTotal(long total) {
    this.total = total;
  }

  public double getMaxScore() {
    return maxScore;
  }

  public void setMaxScore(double maxScore) {
    this.maxScore = maxScore;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.List;
import java.util.Map;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.range.Range;

/**
 * Abstract base class for parsing bucket aggregations from OpenSearch responses. Provides common
 * functionality for extracting key-value pairs from different types of buckets.
 */
public abstract class AbstractBucketAggregationParser
    implements OpenSearchAggregationResponseParser {
  /**
   * Extracts key-value pairs from a composite aggregation bucket without processing its
   * sub-aggregations.
   *
   * <p>For example, for the following CompositeAggregation bucket in response:
   *
   * <pre>{@code
   * {
   *   "key": {
   *     "firstname": "William",
   *     "lastname": "Shakespeare"
   *   },
   *   "sub_agg_name": {
   *     "buckets": []
   *   }
   * }
   * }</pre>
   *
   * It returns {@code {"firstname": "William", "lastname": "Shakespeare"}} as the response.
   *
   * @param bucket the composite aggregation bucket to extract data from
   * @return a map containing the bucket's key-value pairs
   */
  protected Map<String, Object> extract(CompositeAggregation.Bucket bucket) {
    return bucket.getKey();
  }

  /**
   * Extracts key-value pairs from a range aggregation bucket without processing its
   * sub-aggregations.
   *
   * @param bucket the range aggregation bucket to extract data from
   * @param name the name to use as the key in the returned map
   * @return a map containing the bucket's key mapped to the provided name
   */
  protected Map<String, Object> extract(Range.Bucket bucket, String name) {
    return Map.of(name, bucket.getKey());
  }

  @Override
  public List<Map<String, Object>> parse(SearchHits hits) {
    throw new UnsupportedOperationException(this.getClass() + " doesn't support parse(SearchHits)");
  }
}

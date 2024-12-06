/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain.bucketpath;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * The bucket path syntax<br>
 * <AGG_NAME> [ <AGG_SEPARATOR>, <AGG_NAME> ]* [ <METRIC_SEPARATOR>, <METRIC> ]
 *
 * <p>https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline.html#buckets-path-syntax
 */
public class BucketPath {
  private final Deque<Path> pathStack = new ArrayDeque<>();

  public BucketPath add(Path path) {
    if (pathStack.isEmpty()) {
      assert path.isMetricPath() : "The last path in the bucket path must be Metric";
    } else {
      assert path.isAggPath() : "All the other path in the bucket path must be Agg";
    }
    pathStack.push(path);
    return this;
  }

  /** Return the bucket path. Return "", if there is no agg or metric available */
  public String getBucketPath() {
    String bucketPath = pathStack.isEmpty() ? "" : pathStack.pop().getPath();
    return pathStack.stream()
        .map(path -> path.getSeparator() + path.getPath())
        .reduce(bucketPath, String::concat);
  }
}

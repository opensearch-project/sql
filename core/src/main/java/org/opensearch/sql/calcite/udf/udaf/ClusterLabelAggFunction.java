/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.cluster.MatchMode;
import org.opensearch.sql.common.cluster.TextSimilarityClustering;

/**
 * Aggregate function for the cluster command. Uses buffered processing similar to
 * LogPatternAggFunction to handle large datasets efficiently. Processes events in configurable
 * batches to avoid memory issues.
 *
 * <p>When used as a window function over an unbounded frame, the result is a List where each
 * element corresponds to the cluster label for that row position.
 */
public class ClusterLabelAggFunction
    implements UserDefinedAggFunction<ClusterLabelAggFunction.Acc> {

  private static final int DEFAULT_BUFFER_LIMIT = 50000; // rows buffered before a partial merge
  // caps distinct clusters, which bounds the per-row comparison cost
  private static final int DEFAULT_MAX_CLUSTERS = 10000;
  private static final double DEFAULT_THRESHOLD = 0.8;
  private static final String DEFAULT_MATCH_MODE = "termlist";
  private static final String DEFAULT_DELIMS = " ";

  @Override
  public Acc init() {
    return new Acc();
  }

  @Override
  public Object result(Acc acc) {
    return acc.value();
  }

  @Override
  public Acc add(Acc acc, Object... values) {
    // Handle case where Calcite calls generic method with null field value
    if (values.length == 1) {
      String field = (values[0] != null) ? values[0].toString() : null;
      return add(acc, field);
    }

    throw new SyntaxCheckException(
        "Unsupported function signature for cluster aggregate. Valid parameters include (field:"
            + " required string), (t: optional double threshold 0.0-1.0, default 0.8), (match:"
            + " optional string algorithm 'termlist'|'termset'|'ngramset', default 'termlist'),"
            + " (delims: optional string delimiters, default ' '), (labelfield: optional string"
            + " output field name, default 'cluster_label'), (countfield: optional string count"
            + " field name, default 'cluster_count')");
  }

  public Acc add(
      Acc acc,
      String field,
      double threshold,
      String matchMode,
      String delims,
      int bufferLimit,
      int maxClusters) {
    // Store config on the accumulator, not on this function instance, so a reused or shared
    // function object cannot race across concurrent aggregations. Null fields are filtered out
    // upstream in visitCluster; the guard here keeps the label array aligned to the row count.
    acc.configure(threshold, matchMode, delims, maxClusters);
    acc.evaluate(field != null ? field : "");

    if (bufferLimit > 0 && acc.bufferSize() == bufferLimit) {
      acc.partialMerge();
      acc.clearBuffer();
    }

    return acc;
  }

  public Acc add(
      Acc acc, String field, double threshold, String matchMode, String delims, int bufferLimit) {
    return add(acc, field, threshold, matchMode, delims, bufferLimit, DEFAULT_MAX_CLUSTERS);
  }

  public Acc add(Acc acc, String field, double threshold, String matchMode, String delims) {
    return add(
        acc, field, threshold, matchMode, delims, DEFAULT_BUFFER_LIMIT, DEFAULT_MAX_CLUSTERS);
  }

  public Acc add(Acc acc, String field, double threshold, String matchMode) {
    return add(
        acc,
        field,
        threshold,
        matchMode,
        DEFAULT_DELIMS,
        DEFAULT_BUFFER_LIMIT,
        DEFAULT_MAX_CLUSTERS);
  }

  public Acc add(Acc acc, String field, double threshold) {
    return add(
        acc,
        field,
        threshold,
        DEFAULT_MATCH_MODE,
        DEFAULT_DELIMS,
        DEFAULT_BUFFER_LIMIT,
        DEFAULT_MAX_CLUSTERS);
  }

  public Acc add(Acc acc, String field) {
    return add(
        acc,
        field,
        DEFAULT_THRESHOLD,
        DEFAULT_MATCH_MODE,
        DEFAULT_DELIMS,
        DEFAULT_BUFFER_LIMIT,
        DEFAULT_MAX_CLUSTERS);
  }

  public static class Acc implements Accumulator {
    private final List<String> buffer = new ArrayList<>();
    private final List<ClusterRepresentative> globalClusters = new ArrayList<>();
    private final List<Integer> allLabels = new ArrayList<>();
    private int nextClusterId = 1;

    // Per-accumulator config, set on every add. Held here rather than on the function instance
    // so concurrent aggregations do not share mutable state.
    private double threshold = DEFAULT_THRESHOLD;
    private String matchMode = DEFAULT_MATCH_MODE;
    private String delims = DEFAULT_DELIMS;
    private int maxClusters = DEFAULT_MAX_CLUSTERS;

    void configure(double threshold, String matchMode, String delims, int maxClusters) {
      this.threshold = threshold;
      this.matchMode = matchMode;
      this.delims = delims;
      this.maxClusters = maxClusters;
    }

    public int bufferSize() {
      return buffer.size();
    }

    public void evaluate(String value) {
      buffer.add(value != null ? value : "");
    }

    public void partialMerge() {
      if (buffer.isEmpty()) {
        return;
      }

      TextSimilarityClustering clustering =
          new TextSimilarityClustering(threshold, MatchMode.fromString(matchMode), delims);

      for (String value : buffer) {
        ClusterAssignment assignment =
            findOrCreateCluster(value, clustering, threshold, maxClusters);
        allLabels.add(assignment.clusterId);
      }
    }

    private ClusterAssignment findOrCreateCluster(
        String value, TextSimilarityClustering clustering, double threshold, int maxClusters) {
      double bestSimilarity = -1.0;
      ClusterRepresentative bestCluster = null;

      // Compare against existing global clusters
      for (ClusterRepresentative cluster : globalClusters) {
        double similarity = clustering.computeSimilarity(value, cluster.representative);
        if (similarity > bestSimilarity) {
          bestSimilarity = similarity;
          bestCluster = cluster;
        }
      }

      if (bestSimilarity >= threshold - 1e-9 && bestCluster != null) {
        // Join existing cluster
        bestCluster.size++;
        return new ClusterAssignment(bestCluster.id, bestCluster.size);
      } else if (globalClusters.size() < maxClusters) {
        // Create new cluster
        ClusterRepresentative newCluster = new ClusterRepresentative(nextClusterId++, value, 1);
        globalClusters.add(newCluster);
        return new ClusterAssignment(newCluster.id, 1);
      } else {
        // Force into closest existing cluster when at max limit
        if (bestCluster != null) {
          bestCluster.size++;
          return new ClusterAssignment(bestCluster.id, bestCluster.size);
        } else if (!globalClusters.isEmpty()) {
          // Fallback: assign to cluster 1
          globalClusters.get(0).size++;
          return new ClusterAssignment(globalClusters.get(0).id, globalClusters.get(0).size);
        } else {
          // Emergency fallback: create first cluster
          ClusterRepresentative newCluster = new ClusterRepresentative(1, value, 1);
          globalClusters.add(newCluster);
          nextClusterId = 2;
          return new ClusterAssignment(1, 1);
        }
      }
    }

    public void clearBuffer() {
      buffer.clear();
    }

    @Override
    public Object value(Object... argList) {
      partialMerge();
      clearBuffer();
      return new ArrayList<>(allLabels);
    }

    /** Represents a cluster with its representative text and current size */
    private static class ClusterRepresentative {
      final int id;
      final String representative;
      int size;

      ClusterRepresentative(int id, String representative, int size) {
        this.id = id;
        this.representative = representative;
        this.size = size;
      }
    }

    /** Result of cluster assignment */
    private static class ClusterAssignment {
      final int clusterId;
      final int clusterSize;

      ClusterAssignment(int clusterId, int clusterSize) {
        this.clusterId = clusterId;
        this.clusterSize = clusterSize;
      }
    }
  }
}

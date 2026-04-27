/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
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

  private int bufferLimit = 50000; // Configurable buffer size
  private int maxClusters = 10000; // Limit cluster count to prevent memory explosion
  private double threshold = 0.8;
  private String matchMode = "termlist";
  private String delims = " ";

  @Override
  public Acc init() {
    return new Acc();
  }

  @Override
  public Object result(Acc acc) {
    return acc.value(threshold, matchMode, delims, maxClusters);
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
    // Process all rows, even when field is null - convert null to empty string
    // This ensures the result array matches input row count
    String processedField = (field != null) ? field : "";

    this.threshold = threshold;
    this.matchMode = matchMode;
    this.delims = delims;
    this.bufferLimit = bufferLimit;
    this.maxClusters = maxClusters;

    acc.evaluate(processedField);

    if (bufferLimit > 0 && acc.bufferSize() == bufferLimit) {
      acc.partialMerge(threshold, matchMode, delims, maxClusters);
      acc.clearBuffer();
    }

    return acc;
  }

  public Acc add(Acc acc, String field, double threshold, String matchMode, String delims) {
    return add(acc, field, threshold, matchMode, delims, this.bufferLimit, this.maxClusters);
  }

  public Acc add(Acc acc, String field, double threshold, String matchMode) {
    return add(acc, field, threshold, matchMode, this.delims, this.bufferLimit, this.maxClusters);
  }

  public Acc add(Acc acc, String field, double threshold) {
    return add(
        acc, field, threshold, this.matchMode, this.delims, this.bufferLimit, this.maxClusters);
  }

  public Acc add(Acc acc, String field) {
    return add(
        acc,
        field,
        this.threshold,
        this.matchMode,
        this.delims,
        this.bufferLimit,
        this.maxClusters);
  }

  public static class Acc implements Accumulator {
    private final List<String> buffer = new ArrayList<>();
    private final List<ClusterRepresentative> globalClusters = new ArrayList<>();
    private final List<Integer> allLabels = new ArrayList<>();
    private int nextClusterId = 1;

    public int bufferSize() {
      return buffer.size();
    }

    public void evaluate(String value) {
      buffer.add(value != null ? value : "");
    }

    public void partialMerge(Object... argList) {
      if (buffer.isEmpty()) {
        return;
      }

      double threshold = (Double) argList[0];
      String matchMode = (String) argList[1];
      String delims = (String) argList[2];
      int maxClusters = (Integer) argList[3];

      TextSimilarityClustering clustering =
          new TextSimilarityClustering(threshold, matchMode, delims);

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
      partialMerge(argList);
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

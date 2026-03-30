/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
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
    return acc.labels();
  }

  @Override
  public Acc add(Acc acc, Object... values) {
    throw new UnsupportedOperationException("Use typed add method");
  }

  public Acc add(Acc acc, String field, double threshold, String matchMode, String delims) {
    return add(acc, field, threshold, matchMode, delims, bufferLimit, maxClusters);
  }

  public Acc add(
      Acc acc,
      String field,
      double threshold,
      String matchMode,
      String delims,
      int bufferLimit,
      int maxClusters) {
    if (field == null) {
      return acc;
    }

    this.bufferLimit = bufferLimit;
    this.maxClusters = maxClusters;
    this.threshold = threshold;
    this.matchMode = matchMode;
    this.delims = delims;

    acc.addValue(field);
    acc.setParams(threshold, matchMode, delims, maxClusters);

    // Process buffer when it reaches limit (like patterns command)
    if (bufferLimit > 0 && acc.bufferSize() >= bufferLimit) {
      acc.partialProcess();
      acc.clearBuffer();
    }

    return acc;
  }

  /**
   * Accumulator that processes events in batches to avoid memory issues. Thread-safe
   * implementation.
   */
  public static class Acc implements Accumulator {
    // Current buffer being accumulated - using thread-safe collections
    private final List<String> buffer = Collections.synchronizedList(new ArrayList<>());

    // Global cluster state maintained across batches - thread-safe
    private final List<ClusterRepresentative> globalClusters =
        Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> allLabels = Collections.synchronizedList(new ArrayList<>());
    private final List<Integer> allCounts = Collections.synchronizedList(new ArrayList<>());

    private double threshold = 0.8;
    private String matchMode = "termlist";
    private String delims = " ";
    private int maxClusters = 10000;
    private int nextClusterId = 1;

    /** Add value to current buffer */
    public void addValue(String value) {
      buffer.add(value != null ? value : "");
    }

    public void setParams(double threshold, String matchMode, String delims, int maxClusters) {
      this.threshold = threshold;
      this.matchMode = matchMode;
      this.delims = delims;
      this.maxClusters = maxClusters;
    }

    public int bufferSize() {
      return buffer.size();
    }

    /** Process current buffer against existing global clusters */
    public synchronized void partialProcess() {
      if (buffer.isEmpty()) {
        return;
      }

      TextSimilarityClustering clustering =
          new TextSimilarityClustering(threshold, matchMode, delims);

      // Create local copy of buffer to avoid concurrent modification
      List<String> bufferCopy = new ArrayList<>(buffer);

      for (String value : bufferCopy) {
        ClusterAssignment assignment = findOrCreateCluster(value, clustering);
        allLabels.add(assignment.clusterId);
        allCounts.add(assignment.clusterSize);
      }
    }

    /** Find best matching global cluster or create new one - synchronized for thread safety */
    private synchronized ClusterAssignment findOrCreateCluster(
        String value, TextSimilarityClustering clustering) {
      double bestSimilarity = -1.0;
      ClusterRepresentative bestCluster = null;

      // Compare against existing global clusters
      for (ClusterRepresentative cluster : globalClusters) {
        try {
          double similarity = clustering.computeSimilarity(value, cluster.representative);
          if (similarity > bestSimilarity) {
            bestSimilarity = similarity;
            bestCluster = cluster;
          }
        } catch (Exception e) {
          // Log error but continue processing - don't fail entire clustering
          // In production, would use proper logging framework
          System.err.println(
              "Warning: Error computing similarity for cluster "
                  + cluster.id
                  + ": "
                  + e.getMessage());
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

    /** Returns the list of 1-based cluster labels, one per input row. */
    public List<Integer> labels() {
      // Process any remaining buffer
      if (!buffer.isEmpty()) {
        partialProcess();
        clearBuffer();
      }
      return new ArrayList<>(allLabels);
    }

    /** Returns the list of cluster counts, one per input row. */
    public List<Integer> counts() {
      // Process any remaining buffer
      if (!buffer.isEmpty()) {
        partialProcess();
        clearBuffer();
      }
      return new ArrayList<>(allCounts);
    }

    @Override
    public Object value(Object... argList) {
      return labels();
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

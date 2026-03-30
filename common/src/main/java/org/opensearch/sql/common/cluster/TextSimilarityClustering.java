/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.text.similarity.CosineSimilarity;

/**
 * Greedy single-pass text similarity clustering for grouping similar text values.
 * Events are processed in order; each is compared to existing cluster representatives using cosine
 * similarity. If the best match meets the threshold, the event joins that cluster; otherwise a new
 * cluster is created.
 *
 * <p>Optimized for incremental processing with vector caching and memory-efficient operations.
 */
public class TextSimilarityClustering {

  private static final CosineSimilarity COSINE = new CosineSimilarity();

  // Cache vectorized representations to avoid recomputation
  private final Map<String, Map<CharSequence, Integer>> vectorCache = new ConcurrentHashMap<>();
  private static final int MAX_CACHE_SIZE = 10000;

  private final double threshold;
  private final String matchMode;
  private final String delims;

  public TextSimilarityClustering(double threshold, String matchMode, String delims) {
    this.threshold = validateThreshold(threshold);
    this.matchMode = validateMatchMode(matchMode);
    this.delims = delims != null ? delims : " ";
  }

  private static double validateThreshold(double threshold) {
    if (threshold <= 0.0 || threshold >= 1.0) {
      throw new IllegalArgumentException("The threshold must be > 0.0 and < 1.0, got: " + threshold);
    }
    return threshold;
  }

  private static String validateMatchMode(String matchMode) {
    if (matchMode == null) {
      return "termlist";
    }
    switch (matchMode.toLowerCase()) {
      case "termlist":
      case "termset":
      case "ngramset":
        return matchMode.toLowerCase();
      default:
        throw new IllegalArgumentException("Invalid match mode: " + matchMode
            + ". Must be one of: termlist, termset, ngramset");
    }
  }

  /**
   * Compute similarity between two text values using the configured match mode.
   * Used for incremental clustering against cluster representatives.
   */
  public double computeSimilarity(String text1, String text2) {
    if (text1 == null || text2 == null || text1.isEmpty() || text2.isEmpty()) {
      return 0.0;
    }

    Map<CharSequence, Integer> vector1 = vectorizeWithCache(text1);
    Map<CharSequence, Integer> vector2 = vectorizeWithCache(text2);

    return COSINE.cosineSimilarity(vector1, vector2);
  }

  /**
   * Cluster a list of text values. Returns a list of cluster assignments (0-based index into the
   * clusters list) parallel to the input.
   */
  public ClusterResult cluster(List<String> values) {
    List<Map<CharSequence, Integer>> repVectors = new ArrayList<>();
    List<Integer> assignments = new ArrayList<>();
    List<Integer> clusterSizes = new ArrayList<>();

    for (String value : values) {
      Map<CharSequence, Integer> vector = vectorizeWithCache(value);
      int bestCluster = -1;
      double bestSim = -1;

      for (int i = 0; i < repVectors.size(); i++) {
        double sim = COSINE.cosineSimilarity(vector, repVectors.get(i));
        if (sim > bestSim) {
          bestSim = sim;
          bestCluster = i;
        }
      }

      if (bestSim >= threshold - 1e-9 && bestCluster >= 0) {
        assignments.add(bestCluster);
        clusterSizes.set(bestCluster, clusterSizes.get(bestCluster) + 1);
      } else {
        assignments.add(repVectors.size());
        repVectors.add(vector);
        clusterSizes.add(1);
      }
    }

    return new ClusterResult(assignments, clusterSizes);
  }

  /** Vectorize with caching to avoid repeated computation */
  private Map<CharSequence, Integer> vectorizeWithCache(String value) {
    // Clean cache periodically
    cleanCacheIfNeeded();

    // Use cache for common strings to improve performance
    return vectorCache.computeIfAbsent(value, this::vectorize);
  }

  /** Clean cache when it gets too large */
  private void cleanCacheIfNeeded() {
    if (vectorCache.size() > MAX_CACHE_SIZE) {
      // Remove oldest 50% of entries (simple cleanup strategy)
      // In production, could use LRU cache instead
      vectorCache.clear();
    }
  }

  private Map<CharSequence, Integer> vectorize(String value) {
    if (value == null || value.isEmpty()) {
      return Map.of();
    }
    return switch (matchMode) {
      case "termset" -> vectorizeTermSet(value);
      case "ngramset" -> vectorizeNgramSet(value);
      default -> vectorizeTermList(value);
    };
  }

  private static final java.util.regex.Pattern NUMERIC_PATTERN =
      java.util.regex.Pattern.compile("^\\d+$");

  private static String normalizeToken(String token) {
    return NUMERIC_PATTERN.matcher(token).matches() ? "*" : token;
  }

  /** Positional term frequency — token order matters. */
  private Map<CharSequence, Integer> vectorizeTermList(String value) {
    String[] tokens = tokenize(value);
    Map<CharSequence, Integer> vector = new HashMap<>((int) (tokens.length * 1.4));

    for (int i = 0; i < tokens.length; i++) {
      if (!tokens[i].isEmpty()) {
        String key = i + "-" + normalizeToken(tokens[i]);
        vector.merge(key, 1, Integer::sum);
      }
    }
    return vector;
  }

  /** Bag-of-words term frequency — token order ignored. */
  private Map<CharSequence, Integer> vectorizeTermSet(String value) {
    String[] tokens = tokenize(value);
    Map<CharSequence, Integer> vector = new HashMap<>((int) (tokens.length * 1.4));

    for (String token : tokens) {
      if (!token.isEmpty()) {
        vector.merge(normalizeToken(token), 1, Integer::sum);
      }
    }
    return vector;
  }

  /** Character trigram frequency. */
  private Map<CharSequence, Integer> vectorizeNgramSet(String value) {
    if (value.length() < 3) {
      // For very short strings, fall back to character frequency
      Map<CharSequence, Integer> vector = new HashMap<>();
      for (char c : value.toCharArray()) {
        vector.merge(String.valueOf(c), 1, Integer::sum);
      }
      return vector;
    }

    Map<CharSequence, Integer> vector = new HashMap<>((int) ((value.length() - 2) * 1.4));
    for (int i = 0; i <= value.length() - 3; i++) {
      String ngram = value.substring(i, i + 3);
      vector.merge(ngram, 1, Integer::sum);
    }
    return vector;
  }

  private String[] tokenize(String value) {
    if ("non-alphanumeric".equals(delims)) {
      return value.split("[^a-zA-Z0-9_]+");
    }
    String pattern = "[" + java.util.regex.Pattern.quote(delims) + "]+";
    return value.split(pattern);
  }

  /** Result of clustering: parallel assignments and cluster sizes. */
  public static class ClusterResult {
    private final List<Integer> assignments;
    private final List<Integer> clusterSizes;

    public ClusterResult(List<Integer> assignments, List<Integer> clusterSizes) {
      this.assignments = assignments;
      this.clusterSizes = clusterSizes;
    }

    public int getClusterLabel(int eventIndex) {
      return assignments.get(eventIndex) + 1; // Convert to 1-based indexing
    }

    /** Total events in the cluster that the given event belongs to. */
    public int getClusterCount(int eventIndex) {
      return clusterSizes.get(assignments.get(eventIndex));
    }

    public int size() {
      return assignments.size();
    }

    public int numClusters() {
      return clusterSizes.size();
    }
  }
}

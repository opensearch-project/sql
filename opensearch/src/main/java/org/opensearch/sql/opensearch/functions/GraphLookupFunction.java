/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.functions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import org.apache.calcite.rex.RexSubQuery;

/**
 * BFS-based graph traversal function for the graphLookup command.
 *
 * <p>This function performs breadth-first search traversal on a graph represented by rows in a
 * lookup table. It follows edges from starting nodes and collects all reachable nodes up to a
 * specified depth.
 *
 * <p>The algorithm is inspired by MongoDB's $graphLookup operator.
 */
public class GraphLookupFunction {

  /** Internal class to track nodes during BFS with their depth. */
  public record NodeWithDepth(Object value, int depth) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NodeWithDepth that = (NodeWithDepth) o;
      return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }

  /** Result of a single node traversal, including the row data and traversal depth. */
  public record TraversalResult(Object[] row, int depth) {}

  /**
   * Execute BFS graph traversal starting from a given value.
   *
   * @param startValue The starting value to begin traversal from
   * @param lookupTableRows All rows from the lookup table
   * @param fromFieldIndex Index of the field that represents outgoing edges (the field values we
   *     traverse FROM)
   * @param toFieldIndex Index of the field that represents the target to match against (the field
   *     values we traverse TO)
   * @param maxDepth Maximum traversal depth (-1 or 0 for unlimited)
   * @param bidirectional If true, traverse edges in both directions
   * @return List of traversal results containing row data and depth
   */
  public static List<TraversalResult> execute(
      Object startValue,
      List<Object[]> lookupTableRows,
      int fromFieldIndex,
      int toFieldIndex,
      int maxDepth,
      boolean bidirectional) {

    if (startValue == null || lookupTableRows == null || lookupTableRows.isEmpty()) {
      return List.of();
    }

    // Build adjacency index: toField value -> list of rows with matching fromField
    // This creates edges: when we're at a node with fromField=X, we can traverse to nodes
    // where toField=X
    Map<Object, List<Object[]>> forwardAdjacency = new HashMap<>();

    // For bidirectional: also index reverse edges
    // fromField value -> list of rows with matching toField
    Map<Object, List<Object[]>> reverseAdjacency = bidirectional ? new HashMap<>() : null;

    for (Object[] row : lookupTableRows) {
      Object fromValue = row[fromFieldIndex];
      Object toValue = row[toFieldIndex];

      // Forward edge: from fromValue, we can reach this row
      if (fromValue != null) {
        forwardAdjacency.computeIfAbsent(fromValue, k -> new ArrayList<>()).add(row);
      }

      // Reverse edge (for bidirectional): from toValue, we can reach this row
      if (bidirectional && toValue != null) {
        reverseAdjacency.computeIfAbsent(toValue, k -> new ArrayList<>()).add(row);
      }
    }

    // BFS traversal
    List<TraversalResult> results = new ArrayList<>();
    Set<Object> visited = new HashSet<>();
    Queue<NodeWithDepth> queue = new ArrayDeque<>();

    // Start BFS from the starting value
    queue.offer(new NodeWithDepth(startValue, 0));
    visited.add(startValue);

    while (!queue.isEmpty()) {
      NodeWithDepth current = queue.poll();
      int currentDepth = current.depth();

      // Check depth limit
      if (maxDepth > 0 && currentDepth >= maxDepth) {
        continue;
      }

      // Get adjacent nodes via forward edges
      List<Object[]> forwardNeighbors = forwardAdjacency.get(current.value());
      if (forwardNeighbors != null) {
        for (Object[] neighborRow : forwardNeighbors) {
          Object neighborKey = neighborRow[toFieldIndex];
          if (!visited.contains(neighborKey)) {
            visited.add(neighborKey);
            results.add(new TraversalResult(neighborRow, currentDepth + 1));
            queue.offer(new NodeWithDepth(neighborKey, currentDepth + 1));
          }
        }
      }

      // For bidirectional: also traverse reverse edges
      if (bidirectional && reverseAdjacency != null) {
        List<Object[]> reverseNeighbors = reverseAdjacency.get(current.value());
        if (reverseNeighbors != null) {
          for (Object[] neighborRow : reverseNeighbors) {
            Object neighborKey = neighborRow[fromFieldIndex];
            if (!visited.contains(neighborKey)) {
              visited.add(neighborKey);
              results.add(new TraversalResult(neighborRow, currentDepth + 1));
              queue.offer(new NodeWithDepth(neighborKey, currentDepth + 1));
            }
          }
        }
      }
    }

    return results;
  }

  /**
   * Convenience method to get the starting value from an input row.
   *
   * @param inputRow The input row
   * @param toFieldIndex Index of the field in input that contains the starting value
   * @return The starting value for traversal
   */
  public static Object getStartValue(Object[] inputRow, int toFieldIndex) {
    if (inputRow == null || toFieldIndex < 0 || toFieldIndex >= inputRow.length) {
      return null;
    }
    return inputRow[toFieldIndex];
  }

  /**
   * Convert traversal results to an array format suitable for aggregation.
   *
   * @param results List of traversal results
   * @param includeDepth Whether to include depth information in the output
   * @return Array of row arrays (with optional depth appended)
   */
  public static Object[] toResultArray(List<TraversalResult> results, boolean includeDepth) {
    if (results == null || results.isEmpty()) {
      return new Object[0];
    }

    Object[] resultArray = new Object[results.size()];
    for (int i = 0; i < results.size(); i++) {
      TraversalResult result = results.get(i);
      if (includeDepth) {
        // Append depth to the row
        Object[] rowWithDepth = new Object[result.row().length + 1];
        System.arraycopy(result.row(), 0, rowWithDepth, 0, result.row().length);
        rowWithDepth[result.row().length] = result.depth();
        resultArray[i] = rowWithDepth;
      } else {
        resultArray[i] = result.row();
      }
    }
    return resultArray;
  }

  /**
   * Entry point for UDF invocation. Converts List to Object[] and returns results.
   *
   * @param startValue Starting value for BFS traversal
   * @param lookupTable Collected rows from lookup table
   * @param fromIdx Index of from field in lookup rows
   * @param toIdx Index of to field in lookup rows
   * @param maxDepth Maximum traversal depth (-1 = unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   * @param includeDepth Whether to include depth in output rows
   * @return List of result rows as Object arrays
   */
  public static List<Object[]> executeWithDynamicLookup(
      Object startValue,
      RexSubQuery lookupTable,
      int fromIdx,
      int toIdx,
      int maxDepth,
      boolean bidirectional,
      boolean includeDepth) {

    if (lookupTable == null) {
      return List.of();
    }

    // Convert List<?> to List<Object[]>
    List<Object[]> rows = new ArrayList<>();
    for (Object item : List.of()) {
      if (item instanceof Object[] arr) {
        rows.add(arr);
      }
    }

    List<TraversalResult> results =
        execute(startValue, rows, fromIdx, toIdx, maxDepth, bidirectional);

    // Convert to output format
    List<Object[]> output = new ArrayList<>();
    for (TraversalResult result : results) {
      if (includeDepth) {
        Object[] rowWithDepth = new Object[result.row().length + 1];
        System.arraycopy(result.row(), 0, rowWithDepth, 0, result.row().length);
        rowWithDepth[result.row().length] = result.depth();
        output.add(rowWithDepth);
      } else {
        output.add(result.row());
      }
    }
    return output;
  }
}

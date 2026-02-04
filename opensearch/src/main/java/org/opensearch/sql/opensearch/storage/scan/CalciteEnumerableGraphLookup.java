/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.opensearch.index.query.QueryBuilders.termsQuery;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.plan.rel.GraphLookup;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.NamedFieldExpression;
import org.opensearch.sql.opensearch.storage.scan.context.LimitDigest;
import org.opensearch.sql.opensearch.storage.scan.context.OSRequestBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/**
 * Enumerable implementation for graphLookup command.
 *
 * <p>Performs BFS graph traversal by dynamically querying OpenSearch with filter pushdown instead
 * of loading all lookup data into memory. For each source row, it executes BFS queries to find all
 * connected nodes in the graph.
 */
@Getter
public class CalciteEnumerableGraphLookup extends GraphLookup implements EnumerableRel, Scannable {

  /**
   * Creates a CalciteEnumerableGraphLookup.
   *
   * @param cluster Cluster
   * @param traitSet Trait set (must include EnumerableConvention)
   * @param source Source table RelNode
   * @param lookup Lookup table RelNode // * @param lookupIndex OpenSearchIndex for the lookup table
   *     (extracted from lookup RelNode)
   * @param connectFromField Field name for outgoing edges
   * @param connectToField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param depthField Name of the depth field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   */
  public CalciteEnumerableGraphLookup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      RelNode lookup,
      String startWith,
      String connectFromField,
      String connectToField,
      String outputField,
      String depthField,
      int maxDepth,
      boolean bidirectional) {
    super(
        cluster,
        traitSet,
        source,
        lookup,
        startWith,
        connectFromField,
        connectToField,
        outputField,
        depthField,
        maxDepth,
        bidirectional);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CalciteEnumerableGraphLookup(
        getCluster(),
        traitSet,
        inputs.get(0),
        inputs.get(1),
        startWith,
        connectFromField,
        connectToField,
        outputField,
        depthField,
        maxDepth,
        bidirectional);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // TODO: make it more accurate
    return super.computeSelfCost(planner, mq);
  }

  // TODO: support non-scannable inputs
  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            OpenSearchRelOptUtil.replaceDot(getCluster().getTypeFactory(), getRowType()),
            pref.preferArray());

    var scanOperator = implementor.stash(this, CalciteEnumerableGraphLookup.class);
    return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
  }

  @Override
  public Enumerable<@Nullable Object> scan() {
    return new GraphLookupEnumerable(this);
  }

  /** Enumerable implementation that performs BFS traversal for each source row. */
  private static class GraphLookupEnumerable extends AbstractEnumerable<@Nullable Object> {

    private final CalciteEnumerableGraphLookup graphLookup;

    GraphLookupEnumerable(CalciteEnumerableGraphLookup graphLookup) {
      this.graphLookup = graphLookup;
    }

    @Override
    public Enumerator<@Nullable Object> enumerator() {
      return new GraphLookupEnumerator(graphLookup);
    }
  }

  /** Enumerator that performs BFS for each source row. */
  private static class GraphLookupEnumerator implements Enumerator<@Nullable Object> {

    private final CalciteEnumerableGraphLookup graphLookup;
    private final CalciteEnumerableIndexScan lookupScan;
    private final Enumerator<@Nullable Object> sourceEnumerator;
    private final List<String> lookupFields;
    private final int startWithIndex;
    private final int connectFromIdx;
    private final int connectToIdx;

    private Object[] current = null;

    @SuppressWarnings("unchecked")
    GraphLookupEnumerator(CalciteEnumerableGraphLookup graphLookup) {
      this.graphLookup = graphLookup;
      this.lookupScan = (CalciteEnumerableIndexScan) graphLookup.getLookup();
      // For performance consideration, limit the size of the lookup table MaxResultWindow to avoid
      // PIT search
      final int maxResultWindow = this.lookupScan.getOsIndex().getMaxResultWindow();
      this.lookupScan.pushDownContext.add(
          PushDownType.LIMIT,
          new LimitDigest(maxResultWindow, 0),
          (OSRequestBuilderAction)
              requestBuilder -> requestBuilder.pushDownLimit(maxResultWindow, 0));

      // Get the source enumerator
      if (graphLookup.getSource() instanceof Scannable scannable) {
        Enumerable<?> sourceEnum = scannable.scan();
        this.sourceEnumerator = (Enumerator<@Nullable Object>) sourceEnum.enumerator();
      } else {
        throw new IllegalStateException(
            "Source must be Scannable, got: " + graphLookup.getSource().getClass());
      }

      List<String> sourceFields = graphLookup.getSource().getRowType().getFieldNames();
      this.lookupFields = graphLookup.getLookup().getRowType().getFieldNames();
      this.startWithIndex = sourceFields.indexOf(graphLookup.getStartWith());
      this.connectFromIdx = lookupFields.indexOf(graphLookup.connectFromField);
      this.connectToIdx = lookupFields.indexOf(graphLookup.connectToField);
    }

    @Override
    public Object current() {
      // source fields + output array
      return current;
    }

    // TODO: currently we perform BFS for each single row.
    //  We can improve this by performing BFS for batch of rows.
    @Override
    public boolean moveNext() {
      if (!sourceEnumerator.moveNext()) {
        return false;
      }

      // Get current source row
      Object sourceRow = sourceEnumerator.current();
      Object[] sourceValues;

      if (sourceRow instanceof Object[] arr) {
        sourceValues = arr;
      } else {
        // Single column case
        sourceValues = new Object[] {sourceRow};
      }

      // Get the start value for BFS
      Object startValue =
          (startWithIndex >= 0 && startWithIndex < sourceValues.length)
              ? sourceValues[startWithIndex]
              : null;

      // Perform BFS traversal
      List<Object> bfsResults = performBfs(startValue);

      // Build output row: source fields + array of BFS results
      current = new Object[sourceValues.length + 1];
      System.arraycopy(sourceValues, 0, current, 0, sourceValues.length);
      current[sourceValues.length] = bfsResults;

      return true;
    }

    /**
     * Performs BFS traversal starting from the given value by dynamically querying OpenSearch.
     *
     * @param startValue The starting value for BFS
     * @return List of rows found during traversal
     */
    private List<Object> performBfs(Object startValue) {
      if (startValue == null) {
        return List.of();
      }

      // TODO: support spillable for these collections
      List<Object> results = new ArrayList<>();
      Set<Object> visited = new HashSet<>();
      Queue<Object> queue = new ArrayDeque<>();

      // Initialize BFS with start value
      queue.offer(startValue);
      visited.add(startValue);

      int currentLevelDepth = 0;
      while (!queue.isEmpty()) {
        // Collect all values at current level for batch query
        List<Object> currentLevelValues = new ArrayList<>();

        while (!queue.isEmpty()) {
          Object value = queue.poll();
          currentLevelValues.add(value);
        }

        if (currentLevelValues.isEmpty()) {
          break;
        }

        // Query OpenSearch for all current level values
        // Forward direction: connectFromField = currentLevelValues
        List<Object> forwardResults = queryLookupTable(currentLevelValues);

        for (Object row : forwardResults) {
          Object[] rowArray = (Object[]) (row);
          Object nextValue = rowArray[connectFromIdx];
          if (graphLookup.bidirectional && visited.contains(nextValue)) {
            nextValue = rowArray[connectToIdx];
          }
          if (!visited.contains(nextValue)) {
            if (graphLookup.depthField != null) {
              Object[] rowWithDepth = new Object[rowArray.length + 1];
              System.arraycopy(rowArray, 0, rowWithDepth, 0, rowArray.length);
              rowWithDepth[rowArray.length] = currentLevelDepth;
              results.add(rowWithDepth);
            } else {
              results.add(rowArray);
            }

            if (nextValue != null) {
              visited.add(nextValue);
              queue.offer(nextValue);
            }
          }
        }

        if (++currentLevelDepth > graphLookup.maxDepth) break;
      }

      return results;
    }

    /**
     * Queries the lookup table with a terms filter.
     *
     * @param values Values to match
     * @return List of matching rows
     */
    private List<Object> queryLookupTable(List<Object> values) {
      if (values.isEmpty()) {
        return List.of();
      }

      NamedFieldExpression toFieldExpression =
          new NamedFieldExpression(
              connectToIdx, lookupFields, lookupScan.getOsIndex().getFieldTypes());
      QueryBuilder query = termsQuery(toFieldExpression.getReferenceForTermQuery(), values);
      if (graphLookup.bidirectional) {
        NamedFieldExpression fromFieldExpression =
            new NamedFieldExpression(
                connectFromIdx, lookupFields, lookupScan.getOsIndex().getFieldTypes());
        query =
            QueryBuilders.boolQuery()
                .should(query)
                .should(termsQuery(fromFieldExpression.getReferenceForTermQuery(), values));
      }
      CalciteEnumerableIndexScan newScan = (CalciteEnumerableIndexScan) this.lookupScan.copy();
      QueryBuilder finalQuery = query;
      newScan.pushDownContext.add(
          PushDownType.FILTER,
          null,
          (OSRequestBuilderAction)
              requestBuilder -> requestBuilder.pushDownFilterForCalcite(finalQuery));
      Iterator<@Nullable Object> res = newScan.scan().iterator();
      List<Object> results = new ArrayList<>();
      while (res.hasNext()) {
        results.add(res.next());
      }
      return results;
    }

    @Override
    public void reset() {
      sourceEnumerator.reset();
      current = null;
    }

    @Override
    public void close() {
      sourceEnumerator.close();
    }
  }
}

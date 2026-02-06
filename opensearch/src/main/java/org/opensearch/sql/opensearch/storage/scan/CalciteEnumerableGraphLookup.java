/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.termsQuery;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
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
   * @param startField Field name for start entities
   * @param fromField Field name for outgoing edges
   * @param toField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param depthField Name of the depth field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   * @param supportArray Whether to support array-typed fields
   */
  public CalciteEnumerableGraphLookup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      RelNode lookup,
      String startField,
      String fromField,
      String toField,
      String outputField,
      String depthField,
      int maxDepth,
      boolean bidirectional,
      boolean supportArray) {
    super(
        cluster,
        traitSet,
        source,
        lookup,
        startField,
        fromField,
        toField,
        outputField,
        depthField,
        maxDepth,
        bidirectional,
        supportArray);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CalciteEnumerableGraphLookup(
        getCluster(),
        traitSet,
        inputs.get(0),
        inputs.get(1),
        startField,
        fromField,
        toField,
        outputField,
        depthField,
        maxDepth,
        bidirectional,
        supportArray);
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
    private final int startFieldIndex;
    private final int fromFieldIdx;
    private final int toFieldIdx;

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
      this.startFieldIndex = sourceFields.indexOf(graphLookup.getStartField());
      this.fromFieldIdx = lookupFields.indexOf(graphLookup.fromField);
      this.toFieldIdx = lookupFields.indexOf(graphLookup.toField);
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
          (startFieldIndex >= 0 && startFieldIndex < sourceValues.length)
              ? sourceValues[startFieldIndex]
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
      // TODO: If we want to include loop edges, we also need to track the visited edges
      Set<Object> visitedNodes = new HashSet<>();
      Queue<Object> queue = new ArrayDeque<>();

      // Initialize BFS with start value
      if (startValue instanceof List<?> list) {
        list.forEach(
            value -> {
              visitedNodes.add(value);
              queue.offer(value);
            });
      } else {
        visitedNodes.add(startValue);
        queue.offer(startValue);
      }

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
        // Forward direction: fromField = currentLevelValues
        List<Object> forwardResults = queryLookupTable(currentLevelValues, visitedNodes);

        for (Object row : forwardResults) {
          Object[] rowArray = (Object[]) (row);
          Object fromValue = rowArray[fromFieldIdx];
          // Collect next values to traverse (may be single value or list)
          // For forward traversal: extract fromField values for next level
          // For bidirectional: also extract toField values.
          // Skip visited values while keep null value
          List<Object> nextValues = new ArrayList<>();
          collectValues(fromValue, nextValues, visitedNodes);
          if (graphLookup.bidirectional) {
            Object toValue = rowArray[toFieldIdx];
            collectValues(toValue, nextValues, visitedNodes);
          }

          // Add row to results if the nextValues is not empty
          if (!nextValues.isEmpty()) {
            if (graphLookup.depthField != null) {
              Object[] rowWithDepth = new Object[rowArray.length + 1];
              System.arraycopy(rowArray, 0, rowWithDepth, 0, rowArray.length);
              rowWithDepth[rowArray.length] = currentLevelDepth;
              results.add(rowWithDepth);
            } else {
              results.add(rowArray);
            }

            // Add unvisited non-null values to queue for next level traversal
            for (Object val : nextValues) {
              if (val != null) {
                visitedNodes.add(val);
                queue.offer(val);
              }
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
     * @param visitedValues Values to not match (ignored when supportArray is true)
     * @return List of matching rows
     */
    private List<Object> queryLookupTable(
        Collection<Object> values, Collection<Object> visitedValues) {
      if (values.isEmpty()) {
        return List.of();
      }

      // Forward direction query
      QueryBuilder query;
      if (graphLookup.supportArray) {
        // When supportArray is true, don't push down visited filter
        // because array fields may contain multiple values that need to be checked individually
        query = getQueryBuilder(toFieldIdx, values);
      } else {
        query =
            boolQuery()
                .must(getQueryBuilder(toFieldIdx, values))
                .mustNot(getQueryBuilder(fromFieldIdx, visitedValues));
      }

      if (graphLookup.bidirectional) {
        // Also query fromField for bidirectional traversal
        QueryBuilder backQuery;
        if (graphLookup.supportArray) {
          backQuery = getQueryBuilder(fromFieldIdx, values);
        } else {
          backQuery =
              boolQuery()
                  .must(getQueryBuilder(fromFieldIdx, values))
                  .mustNot(getQueryBuilder(toFieldIdx, visitedValues));
        }
        query = QueryBuilders.boolQuery().should(query).should(backQuery);
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

    /**
     * Provides a query builder to search edges with the field matching values
     *
     * @param fieldIdx field index
     * @param values values to match
     * @return query builder
     */
    private QueryBuilder getQueryBuilder(int fieldIdx, Collection<Object> values) {
      String fieldName =
          new NamedFieldExpression(fieldIdx, lookupFields, lookupScan.getOsIndex().getFieldTypes())
              .getReferenceForTermQuery();
      return termsQuery(fieldName, values);
    }

    /**
     * Collects values from a field that may be a single value or a list.
     *
     * @param value The field value (may be single value or List)
     * @param collector The list to collect values into
     * @param visited Previously visited values to avoid duplicates
     */
    private void collectValues(Object value, List<Object> collector, Set<Object> visited) {
      if (value instanceof List<?> list) {
        for (Object item : list) {
          if (!visited.contains(item)) {
            collector.add(item);
          }
        }
      } else if (!visited.contains(value)) {
        collector.add(value);
      }
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

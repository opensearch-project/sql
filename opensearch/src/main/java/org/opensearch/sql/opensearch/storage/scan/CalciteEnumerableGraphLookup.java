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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.plan.rel.GraphLookup;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
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
 *
 * <p><b>All-reached semantics.</b> Every document matched during the traversal is a reached node
 * and is emitted exactly once, at the first (hence minimum) BFS depth it is matched. Emission is
 * independent of whether the document has any unvisited onward neighbors, so a node whose only
 * edges point back to already-visited nodes (for example a leaf of a cycle) is still surfaced. The
 * onward edges are used solely to build the next-level frontier.
 *
 * <p><b>Order independence.</b> The frontier for the next level and the set of reached documents
 * are decided against the visited-node set as it stands at the <em>start</em> of a level; the
 * frontier discovered within a level is folded into the visited set only after the entire level has
 * been processed. The result is therefore independent of the order in which the shard(s) return
 * rows within a level. The order of elements inside the collected output array remains
 * non-contractual.
 *
 * <p><b>Document identity.</b> Reached documents are deduplicated by a structural key over all of
 * their projected columns, not by the connectTo (toField) value, because two distinct documents can
 * share the same toField value and keying on it would collapse them. Residual limitation: two
 * documents whose projected columns are byte-for-byte identical but which are distinct OpenSearch
 * documents (different {@code _id}) still collapse to one result. Fully separating that case
 * requires projecting the document {@code _id} into the lookup scan and stripping it from the
 * user-visible output; that is a cross-layer change (core rel row type, planner rule, metadata
 * projection) and is tracked separately.
 */
@Getter
public class CalciteEnumerableGraphLookup extends GraphLookup implements EnumerableRel, Scannable {
  private static final Logger LOG = LogManager.getLogger();

  /**
   * Creates a CalciteEnumerableGraphLookup.
   *
   * @param cluster Cluster
   * @param traitSet Trait set (must include EnumerableConvention)
   * @param source Source table RelNode
   * @param lookup Lookup table RelNode
   * @param startField Field name for start entities (null in literal start mode)
   * @param startValues Literal start values for top-level graphLookup (null in piped mode)
   * @param fromField Field name for outgoing edges
   * @param toField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param depthField Name of the depth field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   * @param supportArray Whether to support array-typed fields
   * @param batchMode Whether to batch all source start values into a single unified BFS
   * @param usePIT Whether to use PIT (Point In Time) search for complete results
   * @param filter Optional filter condition for lookup table documents
   */
  public CalciteEnumerableGraphLookup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      RelNode lookup,
      @Nullable String startField,
      @Nullable List<Object> startValues,
      String fromField,
      String toField,
      String outputField,
      String depthField,
      int maxDepth,
      boolean bidirectional,
      boolean supportArray,
      boolean batchMode,
      boolean usePIT,
      @Nullable RexNode filter) {
    super(
        cluster,
        traitSet,
        source,
        lookup,
        startField,
        startValues,
        fromField,
        toField,
        outputField,
        depthField,
        maxDepth,
        bidirectional,
        supportArray,
        batchMode,
        usePIT,
        filter);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CalciteEnumerableGraphLookup(
        getCluster(),
        traitSet,
        inputs.get(0),
        inputs.get(1),
        startField,
        startValues,
        fromField,
        toField,
        outputField,
        depthField,
        maxDepth,
        bidirectional,
        supportArray,
        batchMode,
        usePIT,
        filter);
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
    private int startFieldIndex;
    private final int fromFieldIdx;
    private final int toFieldIdx;

    private Object[] current = null;
    private boolean batchModeCompleted = false;
    private boolean literalStartCompleted = false;

    @SuppressWarnings("unchecked")
    GraphLookupEnumerator(CalciteEnumerableGraphLookup graphLookup) {
      this.graphLookup = graphLookup;
      this.lookupScan = (CalciteEnumerableIndexScan) graphLookup.getLookup();
      if (!graphLookup.usePIT) {
        // When usePIT is false (default), limit the size of the lookup table to MaxResultWindow
        // to avoid PIT search for better performance, but results may be incomplete
        final int maxResultWindow = this.lookupScan.getOsIndex().getMaxResultWindow();
        this.lookupScan.pushDownContext.add(
            PushDownType.LIMIT,
            new LimitDigest(maxResultWindow, 0),
            (OSRequestBuilderAction)
                requestBuilder -> requestBuilder.pushDownLimit(maxResultWindow, 0));
      }
      // When usePIT is true, no limit is set, allowing PIT-based pagination for complete results

      // Get the source enumerator (null for literal start mode)
      if (graphLookup.getStartValues() != null) {
        this.sourceEnumerator = null;
        this.startFieldIndex = -1;
      } else if (graphLookup.getSource() instanceof Scannable scannable) {
        Enumerable<?> sourceEnum = scannable.scan();
        this.sourceEnumerator = (Enumerator<@Nullable Object>) sourceEnum.enumerator();
      } else {
        throw new IllegalStateException(
            "Source must be Scannable, got: " + graphLookup.getSource().getClass());
      }

      try {
        this.lookupFields = graphLookup.getLookup().getRowType().getFieldNames();
        this.fromFieldIdx = lookupFields.indexOf(graphLookup.fromField);
        this.toFieldIdx = lookupFields.indexOf(graphLookup.toField);

        if (graphLookup.getStartValues() == null) {
          List<String> sourceFields = graphLookup.getSource().getRowType().getFieldNames();
          this.startFieldIndex = sourceFields.indexOf(graphLookup.getStartField());
        }

        // Push down user-specified filter to the lookup scan
        if (graphLookup.filter != null) {
          List<String> schema = graphLookup.getLookup().getRowType().getFieldNames();
          Map<String, ExprType> fieldTypes = this.lookupScan.getOsIndex().getAllFieldTypes();
          try {
            QueryBuilder filterQuery =
                PredicateAnalyzer.analyze(graphLookup.filter, schema, fieldTypes);
            this.lookupScan.pushDownContext.add(
                PushDownType.FILTER,
                null,
                (OSRequestBuilderAction) rb -> rb.pushDownFilterForCalcite(filterQuery));
          } catch (PredicateAnalyzer.ExpressionNotAnalyzableException e) {
            throw new RuntimeException(
                "Cannot push down filter for graphLookup: " + e.getMessage(), e);
          }
        }
      } catch (Exception e) {
        if (sourceEnumerator != null) {
          sourceEnumerator.close();
        }
        throw e;
      }
    }

    @Override
    public Object current() {
      // Literal start mode: single column output, Calcite expects scalar value
      if (graphLookup.getStartValues() != null) {
        return current[0];
      }
      // source fields + output array (normal mode) or [source array, lookup array] (batch mode)
      return current;
    }

    @Override
    public boolean moveNext() {
      if (graphLookup.getStartValues() != null) {
        return moveNextLiteralStartMode();
      } else if (graphLookup.batchMode) {
        return moveNextBatchMode();
      } else {
        return moveNextNormalMode();
      }
    }

    /**
     * Literal start mode: perform single BFS seeded with all literal start values, return one row.
     */
    private boolean moveNextLiteralStartMode() {
      if (literalStartCompleted) {
        return false;
      }
      literalStartCompleted = true;

      // Perform single BFS seeded with all literal start values
      List<Object> bfsResults = performBfs(graphLookup.getStartValues());

      // Output single row: just the hierarchy array
      current = new Object[] {bfsResults};
      return true;
    }

    /**
     * Batch mode: collect all source start values, perform unified BFS, return single aggregated
     * row.
     */
    private boolean moveNextBatchMode() {
      // Batch mode only returns one row
      if (batchModeCompleted) {
        return false;
      }
      batchModeCompleted = true;

      // Collect all source rows and start values
      List<Object> allSourceRows = new ArrayList<>();
      Set<Object> allStartValues = new HashSet<>();

      while (sourceEnumerator.moveNext()) {
        Object sourceRow = sourceEnumerator.current();
        Object[] sourceValues;

        if (sourceRow instanceof Object[] arr) {
          sourceValues = arr;
        } else {
          sourceValues = new Object[] {sourceRow};
        }

        // Store the source row
        allSourceRows.add(sourceValues);

        // Collect start value(s)
        Object startValue =
            (startFieldIndex >= 0 && startFieldIndex < sourceValues.length)
                ? sourceValues[startFieldIndex]
                : null;

        if (startValue != null) {
          if (startValue instanceof List<?> list) {
            allStartValues.addAll(list);
          } else {
            allStartValues.add(startValue);
          }
        }
      }

      // Perform unified BFS with all start values
      List<Object> bfsResults = performBfs(allStartValues);

      // Build output row: [Array<source>, Array<lookup>]
      current = new Object[] {allSourceRows, bfsResults};

      return true;
    }

    /** Normal mode: perform BFS for each source row individually. */
    private boolean moveNextNormalMode() {
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
      // Node key VALUES that have been folded into the traversal frontier. A value enters this set
      // exactly once, the first time it is discovered. This is what makes the BFS terminate on
      // cyclic graphs and what guarantees every node is assigned its shallowest (minimum) depth.
      // TODO: If we want to include loop edges, we also need to track the visited edges
      Set<Object> visitedNodes = new HashSet<>();
      // Identities of documents already emitted into results. Every matched document is emitted
      // once, at the first (minimum) depth it is matched. Documents are deduplicated by a
      // structural key over all projected columns rather than by the toField (connectTo) value,
      // because two distinct documents can share the same toField value and keying on it would
      // collapse them. See the class-level note for the residual identical-duplicate (_id) case.
      Set<Object> emitted = new HashSet<>();
      Queue<Object> queue = new ArrayDeque<>();

      // Initialize BFS with the start value(s).
      if (startValue instanceof Collection<?> collection) {
        collection.forEach(
            value -> {
              if (visitedNodes.add(value)) {
                queue.offer(value);
              }
            });
      } else {
        visitedNodes.add(startValue);
        queue.offer(startValue);
      }

      int currentLevelDepth = 0;
      while (!queue.isEmpty()) {
        // Drain all values at the current level for a single batched query.
        List<Object> currentLevelValues = new ArrayList<>();
        while (!queue.isEmpty()) {
          currentLevelValues.add(queue.poll());
        }
        if (currentLevelValues.isEmpty()) {
          break;
        }

        List<Object> forwardResults = queryLookupTable(currentLevelValues);

        if (!graphLookup.usePIT
            && forwardResults.size() >= this.lookupScan.getOsIndex().getMaxResultWindow()) {
          LOG.warn("BFS result size exceeds max result window, returning partial result.");
        }

        // The next-level frontier is decided against visitedNodes as it stands at the START of this
        // level, and is folded into visitedNodes only AFTER the whole level has been processed
        // (below). This removes any dependence on the order in which the shard(s) return rows
        // within
        // a level. A LinkedHashSet collapses converging paths (two frontier nodes reaching the same
        // target in one level) into a single next-level entry.
        Set<Object> nextFrontier = new LinkedHashSet<>();
        for (Object row : forwardResults) {
          Object[] rowArray = (Object[]) row;

          // All-reached semantics: every matched document is a reached node and is emitted once, at
          // the first (hence minimum) depth it is seen. Emission is NOT gated on whether the
          // document has unvisited onward neighbors, so a node whose only edges point back to
          // already-visited nodes is still surfaced.
          if (emitted.add(identityKey(rowArray))) {
            if (graphLookup.depthField != null) {
              Object[] rowWithDepth = new Object[rowArray.length + 1];
              System.arraycopy(rowArray, 0, rowWithDepth, 0, rowArray.length);
              rowWithDepth[rowArray.length] = currentLevelDepth;
              results.add(rowWithDepth);
            } else {
              results.add(rowArray);
            }
          }

          // Onward edges only build the next frontier; they never affect whether the current
          // document is emitted. Forward traversal follows fromField; bidirectional additionally
          // follows toField. Null edges are graph terminals and are never enqueued.
          addToFrontier(rowArray[fromFieldIdx], nextFrontier, visitedNodes);
          if (graphLookup.bidirectional) {
            addToFrontier(rowArray[toFieldIdx], nextFrontier, visitedNodes);
          }
        }

        // Fold this level's frontier into visitedNodes and enqueue it for the next level.
        for (Object value : nextFrontier) {
          visitedNodes.add(value);
          queue.offer(value);
        }

        if (++currentLevelDepth > graphLookup.maxDepth) {
          break;
        }
      }

      return results;
    }

    /**
     * Queries the lookup table for documents reachable from the current frontier.
     *
     * <p>Matches documents whose toField (connectTo) is one of the frontier {@code values}; for
     * bidirectional traversal it additionally matches documents whose fromField is one of the
     * values. No visited-node exclusion is applied: a reached document must be returned even when
     * its onward neighbors were already visited, otherwise all-reached emission could not surface
     * it. The traversal still terminates because a value enters the frontier at most once (guarded
     * by visitedNodes in {@link #addToFrontier}), so each value is queried at most once across the
     * whole traversal.
     *
     * @param values frontier values to match
     * @return list of matching rows
     */
    private List<Object> queryLookupTable(Collection<Object> values) {
      if (values.isEmpty()) {
        return List.of();
      }

      QueryBuilder query = getQueryBuilder(toFieldIdx, values);
      if (graphLookup.bidirectional) {
        QueryBuilder backQuery = getQueryBuilder(fromFieldIdx, values);
        query = boolQuery().should(query).should(backQuery);
      }
      CalciteEnumerableIndexScan newScan = (CalciteEnumerableIndexScan) this.lookupScan.copy();
      QueryBuilder finalQuery = query;
      newScan.pushDownContext.add(
          PushDownType.FILTER,
          null,
          (OSRequestBuilderAction)
              requestBuilder -> requestBuilder.pushDownFilterForCalcite(finalQuery));
      Iterator<@Nullable Object> res = newScan.scan().iterator();
      try {
        List<Object> results = new ArrayList<>();
        while (res.hasNext()) {
          results.add(res.next());
        }
        return results;
      } finally {
        closeIterator(res);
      }
    }

    private static <T> void closeIterator(@Nullable Iterator<? extends T> iterator) {
      if (iterator instanceof AutoCloseable) {
        try {
          ((AutoCloseable) iterator).close();
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
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
     * Adds the unvisited, non-null nodes reachable through an edge value to the next-level
     * frontier. The edge value may be a single value or a list (array-typed edge field). Null
     * members are graph terminals and are skipped. Membership is tested against {@code visited} as
     * of the start of the current level, so sibling rows within a level cannot influence one
     * another.
     *
     * @param edgeValue the edge field value (single value or list)
     * @param frontier the next-level frontier to add to
     * @param visited nodes already folded into the traversal as of the start of the level
     */
    private void addToFrontier(Object edgeValue, Set<Object> frontier, Set<Object> visited) {
      if (edgeValue instanceof List<?> list) {
        for (Object item : list) {
          if (item != null && !visited.contains(item)) {
            frontier.add(item);
          }
        }
      } else if (edgeValue != null && !visited.contains(edgeValue)) {
        frontier.add(edgeValue);
      }
    }

    /**
     * Builds a structural identity key for a lookup row over all of its projected columns. Nested
     * array cells (Object[] or List) are canonicalized recursively so that value-equal rows produce
     * equal keys. This deduplicates a document against itself when it is rediscovered at a deeper
     * level and, unlike keying on the toField value alone, keeps two distinct documents that merely
     * share a toField value as separate results.
     *
     * <p>Residual limitation: two documents whose projected columns are byte-for-byte identical but
     * which are distinct OpenSearch documents (different {@code _id}) still collapse to one key.
     * See the class-level note.
     *
     * @param rowArray the raw lookup row (before any depth column is appended)
     * @return an equals/hashCode-stable structural key
     */
    private static Object identityKey(Object[] rowArray) {
      List<Object> key = new ArrayList<>(rowArray.length);
      for (Object cell : rowArray) {
        key.add(canonicalize(cell));
      }
      return key;
    }

    private static Object canonicalize(Object value) {
      if (value instanceof Object[] arr) {
        List<Object> list = new ArrayList<>(arr.length);
        for (Object item : arr) {
          list.add(canonicalize(item));
        }
        return list;
      }
      if (value instanceof List<?> list) {
        List<Object> canonical = new ArrayList<>(list.size());
        for (Object item : list) {
          canonical.add(canonicalize(item));
        }
        return canonical;
      }
      return value;
    }

    @Override
    public void reset() {
      if (sourceEnumerator != null) {
        sourceEnumerator.reset();
      }
      current = null;
      literalStartCompleted = false;
    }

    @Override
    public void close() {
      if (sourceEnumerator != null) {
        sourceEnumerator.close();
      }
    }
  }
}

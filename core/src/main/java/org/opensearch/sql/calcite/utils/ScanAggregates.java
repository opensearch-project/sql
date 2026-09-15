/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Analyzes how a plan's aggregates read scans, so the partial-result pushdown can keep one coherent
 * index subset per scan when several aggregates read it.
 *
 * <p>Partial-result mode narrows an aggregation to the indices where its group fields are
 * aggregatable, per aggregation. When a plan groups one scan more than once -- {@code chart} (rows
 * plus a top-N ranking pass), or {@code append}/{@code appendcol}/{@code multisearch}/{@code join}
 * over aggregating subsearches -- each aggregation would pick its own subset, so the one response
 * would mix document populations. To avoid that, every aggregation over a given scan is narrowed to
 * the same subset: the indices aggregatable for the <b>union</b> of the group fields of all those
 * aggregations. That union is exactly what {@code PartialResultAggregatePushdown.plan} needs, so
 * the mapping-aware half stays in the opensearch module; this half only resolves group keys to
 * field names, which is Calcite-only.
 */
public final class ScanAggregates {

  private static final Logger LOG = LogManager.getLogger(ScanAggregates.class);

  private ScanAggregates() {}

  /**
   * The scans that more than one grouped aggregate reads, keyed by the scan's qualified name.
   *
   * @param unionFieldsByTable for a scan the analysis resolved fully, the sorted union of the group
   *     fields of every aggregate reading it; the partial-result pushdown narrows every aggregate
   *     over that scan to the indices aggregatable for all of them
   * @param barredTables scans read by several grouped aggregates that the analysis could not
   *     resolve (a group key with no traceable column origin); partial mode must be skipped for
   *     them so the response stays complete rather than risk mixing populations
   * @param barAll set when the analysis itself failed; partial mode is skipped for the whole query,
   *     the safe fallback
   */
  public record MultiAggregateInfo(
      Map<String, List<String>> unionFieldsByTable, Set<String> barredTables, boolean barAll) {

    public static final MultiAggregateInfo EMPTY =
        new MultiAggregateInfo(Map.of(), Set.of(), false);
    private static final MultiAggregateInfo BAR_ALL =
        new MultiAggregateInfo(Map.of(), Set.of(), true);

    /** The union of group fields to narrow this scan by, or null when it is not multi-aggregate. */
    public List<String> unionFieldsFor(String qualifiedName) {
      return unionFieldsByTable.get(qualifiedName);
    }

    /**
     * Whether partial mode must be skipped for this scan (unresolved multi-aggregate, or bar-all).
     */
    public boolean barred(String qualifiedName) {
      return barAll || barredTables.contains(qualifiedName);
    }
  }

  /**
   * Resolve, for each scan read by more than one grouped aggregate, the union of those aggregates'
   * group fields. A scan read by a single grouped aggregate is left out: it keeps the per-aggregate
   * pushdown, unchanged. Never throws: on any failure it returns {@link
   * MultiAggregateInfo#BAR_ALL}, so partial mode is skipped and the complete result is returned.
   */
  public static MultiAggregateInfo analyze(RelNode plan) {
    // Most queries group a scan at most once; skip the column-origin resolution for them.
    if (!moreThanOneGroupedOverAScan(plan)) {
      return MultiAggregateInfo.EMPTY;
    }
    try {
      RelMetadataQuery mq = plan.getCluster().getMetadataQuery();
      Map<String, Integer> groupedCountByTable = new HashMap<>();
      Map<String, Set<String>> fieldsByTable = new HashMap<>();
      Set<String> unresolved = new HashSet<>();

      for (RelNode node : nodesOf(plan)) {
        if (!(node instanceof Aggregate aggregate) || aggregate.getGroupSet().isEmpty()) {
          continue;
        }
        Map<String, Set<String>> origins = groupFieldOrigins(aggregate, mq);
        if (origins == null) {
          // Reaches scans but the group keys are not traceable -> bar those scans.
          for (String qn : scanQualifiedNames(aggregate)) {
            groupedCountByTable.merge(qn, 1, Integer::sum);
            unresolved.add(qn);
          }
        } else {
          origins.forEach(
              (qn, fields) -> {
                groupedCountByTable.merge(qn, 1, Integer::sum);
                fieldsByTable.computeIfAbsent(qn, k -> new LinkedHashSet<>()).addAll(fields);
              });
        }
      }

      Map<String, List<String>> unionFields = new HashMap<>();
      Set<String> barred = new HashSet<>();
      groupedCountByTable.forEach(
          (qn, count) -> {
            if (count <= 1) {
              return; // single grouped aggregate -> per-aggregate pushdown, unchanged
            }
            if (unresolved.contains(qn)) {
              barred.add(qn);
            } else {
              List<String> fields = new ArrayList<>(fieldsByTable.getOrDefault(qn, Set.of()));
              Collections.sort(fields);
              unionFields.put(qn, fields);
            }
          });
      return new MultiAggregateInfo(unionFields, barred, false);
    } catch (Exception e) {
      LOG.debug("Cannot analyze scan aggregates, barring partial results for the query", e);
      return MultiAggregateInfo.BAR_ALL;
    }
  }

  /**
   * The group fields of one aggregate, keyed by the qualified name of the scan they originate from.
   * Returns null if any group key has no traceable column origin (e.g. a constant), so the caller
   * can bar rather than narrow by an incomplete field set.
   */
  private static Map<String, Set<String>> groupFieldOrigins(
      Aggregate aggregate, RelMetadataQuery mq) {
    Map<String, Set<String>> byTable = new HashMap<>();
    RelNode input = aggregate.getInput();
    for (int group : aggregate.getGroupSet()) {
      Set<RelColumnOrigin> origins = mq.getColumnOrigins(input, group);
      if (origins == null || origins.isEmpty()) {
        return null;
      }
      for (RelColumnOrigin origin : origins) {
        RelOptTable table = origin.getOriginTable();
        if (table == null) {
          return null;
        }
        String field = table.getRowType().getFieldNames().get(origin.getOriginColumnOrdinal());
        byTable.computeIfAbsent(qualifiedName(table), k -> new LinkedHashSet<>()).add(field);
      }
    }
    return byTable;
  }

  /**
   * Structural check kept for callers that only need "does the plan group a scan more than once".
   */
  public static boolean moreThanOneGroupedOverAScan(RelNode plan) {
    try {
      int grouped = 0;
      for (RelNode node : nodesOf(plan)) {
        if (node instanceof Aggregate aggregate
            && !aggregate.getGroupSet().isEmpty()
            && reachesAScan(aggregate)
            && ++grouped > 1) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      LOG.debug("Cannot count scan aggregates, barring partial results", e);
      return true;
    }
  }

  private static boolean reachesAScan(RelNode root) {
    for (RelNode node : nodesOf(root)) {
      if (node instanceof TableScan) {
        return true;
      }
    }
    return false;
  }

  private static Set<String> scanQualifiedNames(RelNode root) {
    Set<String> names = new HashSet<>();
    for (RelNode node : nodesOf(root)) {
      if (node instanceof TableScan scan) {
        names.add(qualifiedName(scan.getTable()));
      }
    }
    return names;
  }

  private static String qualifiedName(RelOptTable table) {
    return String.join(".", table.getQualifiedName());
  }

  /**
   * Collects {@code root} and everything below it, iteratively -- this runs on query plans, which
   * nest arbitrarily deep. Plans are DAGs: chart hands one aggregate to both its data branch and
   * its ranking branch, so nodes are collected by identity, or a shared aggregate would be counted
   * once per path reaching it.
   */
  private static List<RelNode> nodesOf(RelNode root) {
    Set<RelNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    List<RelNode> collected = new ArrayList<>();
    Deque<RelNode> pending = new ArrayDeque<>(List.of(root));
    while (!pending.isEmpty()) {
      RelNode node = pending.pop();
      if (!visited.add(node)) {
        continue;
      }
      collected.add(node);
      pending.addAll(node.getInputs());
      // A subquery becomes a join later, so its scan takes pushdown too.
      collectSubQueryPlans(node, pending);
    }
    return collected;
  }

  private static void collectSubQueryPlans(RelNode node, Deque<RelNode> target) {
    node.accept(
        new RexShuttle() {
          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            target.add(subQuery.rel);
            return super.visitSubQuery(subQuery);
          }
        });
  }
}

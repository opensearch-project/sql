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
 * Resolves, per scan, the union of the group fields of every aggregate reading it, so
 * partial-result pushdown can narrow them all to one shared index subset. When a plan groups a scan
 * more than once (chart's rows plus its top-N ranking, or append/appendcol/multisearch/join over
 * aggregating subsearches) a per-aggregate subset would let one response mix document populations.
 * Resolves field names only (Calcite-only); the opensearch module maps them to indices.
 */
public final class ScanAggregates {

  private static final Logger LOG = LogManager.getLogger(ScanAggregates.class);

  private ScanAggregates() {}

  /**
   * Scans read by more than one grouped aggregate, keyed by qualified name.
   *
   * @param unionFieldsByTable sorted union of group fields to narrow each resolved scan by
   * @param barredTables scans with an unresolvable group key; skip partial mode (stay complete)
   * @param barAll analysis failed; skip partial mode for the whole query
   */
  public record MultiAggregateInfo(
      Map<String, List<String>> unionFieldsByTable, Set<String> barredTables, boolean barAll) {

    public static final MultiAggregateInfo EMPTY =
        new MultiAggregateInfo(Map.of(), Set.of(), false);
    private static final MultiAggregateInfo BAR_ALL =
        new MultiAggregateInfo(Map.of(), Set.of(), true);

    /** Fields to narrow this scan by, or null when it is not read by several grouped aggregates. */
    public List<String> unionFieldsFor(String qualifiedName) {
      return unionFieldsByTable.get(qualifiedName);
    }

    public boolean barred(String qualifiedName) {
      return barAll || barredTables.contains(qualifiedName);
    }
  }

  /** Never throws; returns {@link MultiAggregateInfo#BAR_ALL} on any failure (stays complete). */
  public static MultiAggregateInfo analyze(RelNode plan) {
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
              return; // single grouped aggregate keeps the per-aggregate pushdown
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

  /** Group fields keyed by origin scan; null if any group key has no traceable column origin. */
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

  /** The cheap structural gate before {@link #analyze}. */
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

  /** By identity: plans are DAGs (chart shares one aggregate across branches). */
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
      collectSubQueryPlans(node, pending); // a subquery becomes a join, so its scan takes pushdown
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

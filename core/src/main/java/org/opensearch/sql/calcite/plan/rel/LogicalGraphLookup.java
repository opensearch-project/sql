/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * Logical RelNode for graphLookup command. TODO: need to support trim fields and several transpose
 * rules for this new added RelNode
 */
@Getter
public class LogicalGraphLookup extends GraphLookup {

  /**
   * Creates a LogicalGraphLookup.
   *
   * @param cluster Cluster
   * @param traitSet Trait set
   * @param source Source table RelNode
   * @param lookup Lookup table RelNode
   * @param startField Field name for start entities
   * @param fromField Field name for outgoing edges
   * @param toField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param depthField Name of the depth field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   * @param supportArray Whether to support array-typed fields
   * @param batchMode Whether to batch all source start values into a single unified BFS
   * @param usePIT Whether to use PIT (Point In Time) search for complete results
   */
  protected LogicalGraphLookup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      RelNode lookup,
      String startField,
      String fromField,
      String toField,
      String outputField,
      @Nullable String depthField,
      int maxDepth,
      boolean bidirectional,
      boolean supportArray,
      boolean batchMode,
      boolean usePIT) {
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
        supportArray,
        batchMode,
        usePIT);
  }

  /**
   * Creates a LogicalGraphLookup with Convention.NONE.
   *
   * @param source Source table RelNode
   * @param lookup Lookup table RelNode
   * @param startField Field name for start entities
   * @param fromField Field name for outgoing edges
   * @param toField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param depthField Named of the output depth field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   * @param supportArray Whether to support array-typed fields
   * @param batchMode Whether to batch all source start values into a single unified BFS
   * @param usePIT Whether to use PIT (Point In Time) search for complete results
   * @return A new LogicalGraphLookup instance
   */
  public static LogicalGraphLookup create(
      RelNode source,
      RelNode lookup,
      String startField,
      String fromField,
      String toField,
      String outputField,
      @Nullable String depthField,
      int maxDepth,
      boolean bidirectional,
      boolean supportArray,
      boolean batchMode,
      boolean usePIT) {
    RelOptCluster cluster = source.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalGraphLookup(
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
        supportArray,
        batchMode,
        usePIT);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LogicalGraphLookup(
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
        supportArray,
        batchMode,
        usePIT);
  }
}

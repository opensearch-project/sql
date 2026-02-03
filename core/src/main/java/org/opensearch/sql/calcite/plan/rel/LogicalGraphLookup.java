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

/** Logical RelNode for graphLookup command. */
@Getter
public class LogicalGraphLookup extends GraphLookup {

  /**
   * Creates a LogicalGraphLookup.
   *
   * @param cluster Cluster
   * @param traitSet Trait set
   * @param source Source table RelNode
   * @param lookup Lookup table RelNode
   * @param connectFromField Field name for outgoing edges
   * @param connectToField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param depthField Name of the depth field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   */
  protected LogicalGraphLookup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      RelNode lookup,
      String startWith,
      String connectFromField,
      String connectToField,
      String outputField,
      @Nullable String depthField,
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

  /**
   * Creates a LogicalGraphLookup with Convention.NONE.
   *
   * @param source Source table RelNode
   * @param lookup Lookup table RelNode
   * @param startWith Field name for start with entities
   * @param connectFromField Field name for outgoing edges
   * @param connectToField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   * @param depthField Named of the output depth field
   * @return A new LogicalGraphLookup instance
   */
  public static LogicalGraphLookup create(
      RelNode source,
      RelNode lookup,
      String startWith,
      String connectFromField,
      String connectToField,
      String outputField,
      @Nullable String depthField,
      int maxDepth,
      boolean bidirectional) {
    RelOptCluster cluster = source.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalGraphLookup(
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
    return new LogicalGraphLookup(
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
}

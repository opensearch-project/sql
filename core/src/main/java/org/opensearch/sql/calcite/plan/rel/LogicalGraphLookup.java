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
import org.apache.calcite.rex.RexNode;

/**
 * Logical RelNode for graphLookup command. TODO: need to support trim fields and several transpose
 * rules for this new added RelNode
 */
@Getter
public class LogicalGraphLookup extends GraphLookup {

  protected LogicalGraphLookup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      RelNode lookup,
      @Nullable String startField,
      @Nullable List<Object> startValues,
      String fromField,
      String toField,
      String outputField,
      @Nullable String depthField,
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

  public static LogicalGraphLookup create(
      RelNode source,
      RelNode lookup,
      @Nullable String startField,
      @Nullable List<Object> startValues,
      String fromField,
      String toField,
      String outputField,
      @Nullable String depthField,
      int maxDepth,
      boolean bidirectional,
      boolean supportArray,
      boolean batchMode,
      boolean usePIT,
      @Nullable RexNode filter) {
    RelOptCluster cluster = source.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalGraphLookup(
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
    return new LogicalGraphLookup(
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
}

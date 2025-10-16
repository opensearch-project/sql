/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.util.mapping.AbstractTargetMapping;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

/** A special RexShuttle to remap output project indices after script project pushdown. */
public class RexPermuteSelectedColumnShuttle extends RexPermuteInputsShuttle {
  private final SelectedColumnTargetMapping mapping;

  public RexPermuteSelectedColumnShuttle(TargetMapping mapping) {
    super(mapping);
    this.mapping = (SelectedColumnTargetMapping) mapping;
  }

  /**
   * Creates a RexShuttle to remap output project indices after script project pushdown.
   *
   * @param mapping Target mapping for selected column
   */
  public static RexPermuteSelectedColumnShuttle of(SelectedColumnTargetMapping mapping) {
    return new RexPermuteSelectedColumnShuttle(mapping);
  }

  /**
   * Internal RexInputRef remapping method. Since new projected fields are pushed, we need to change
   * original scan's input reference index to new pushed scan's field index.
   *
   * @param local original local RexInputRef
   * @return new RexInputRef with remapped index
   */
  @Override
  public RexNode visitInputRef(RexInputRef local) {
    final int index = local.getIndex();
    int target = mapping.getTarget(index);
    return new RexInputRef(target, local.getType());
  }

  /**
   * Handle a special case when DERIVED_NEW field is created after project pushdown. It doesn't need
   * to visit RexInputRef to change index. Directly create RexInputRef point to a pushed down
   * derived field index.
   *
   * @param exprs input project expression list that needs remapping indices
   * @param out changed index new project expression list
   */
  @Override
  public void visitList(Iterable<? extends RexNode> exprs, List<RexNode> out) {
    int index = 0;
    for (RexNode expr : exprs) {
      int derivedIndex = mapping.getTargetDerivedIndex(index);
      if (derivedIndex != -1) {
        out.add(new RexInputRef(derivedIndex, expr.getType()));
      } else {
        out.add(expr.accept(this));
      }
      index++;
    }
  }

  /**
   * A target mapping to record original scan's index to new pushed expression index for physical
   * and derived_existing fields. For new derived fields after pushdown, record the mapping of
   * target index to its index in new scan's schema.
   */
  public static class SelectedColumnTargetMapping extends AbstractTargetMapping {
    private final Map<Integer, Integer> originalScanSourceToTarget;
    private final Map<Integer, Integer> targetToDerived;

    protected SelectedColumnTargetMapping(
        Map<Integer, Integer> originalScanSourceToTarget,
        Map<Integer, Integer> targetToDerived,
        int targetCount,
        int sourceCount) {
      super(sourceCount, targetCount);
      this.originalScanSourceToTarget = originalScanSourceToTarget;
      this.targetToDerived = targetToDerived;
    }

    /**
     * Create a selected column target mapping based on a list of selected columns and original
     * source count like scan's field size.
     *
     * @param selectedColumns a list of SelectedColumn
     * @param sourceCount source count
     * @return new instance of SelectedColumnTargetMapping
     */
    public static SelectedColumnTargetMapping create(
        List<SelectedColumn> selectedColumns, int sourceCount) {
      final Map<Integer, Integer> originalScanSourceToTarget = new HashMap<>();
      final Map<Integer, Integer> targetToDerived = new HashMap<>();

      for (int i = 0; i < selectedColumns.size(); i++) {
        SelectedColumn selectedColumn = selectedColumns.get(i);
        switch (selectedColumn.getKind()) {
          case PHYSICAL:
          case DERIVED_EXISTING:
            originalScanSourceToTarget.put(selectedColumn.getOldIdx(), i);
            break;
          case DERIVED_NEW:
            targetToDerived.put(selectedColumn.getProjIdx(), i);
            break;
        }
      }
      return new SelectedColumnTargetMapping(
          originalScanSourceToTarget, targetToDerived, selectedColumns.size(), sourceCount);
    }

    @Override
    public int getTargetOpt(int source) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getTarget(int source) {
      return originalScanSourceToTarget.get(source);
    }

    public int getTargetDerivedIndex(int projIndex) {
      return targetToDerived.getOrDefault(projIndex, -1);
    }
  }
}

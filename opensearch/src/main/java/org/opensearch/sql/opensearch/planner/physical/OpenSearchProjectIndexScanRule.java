/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.opensearch.storage.scan.SelectedColumn;
import org.opensearch.sql.opensearch.storage.scan.SelectedColumn.Kind;

/** Planner rule that push a {@link LogicalProject} down to {@link CalciteLogicalIndexScan} */
@Value.Enclosing
public class OpenSearchProjectIndexScanRule extends RelRule<OpenSearchProjectIndexScanRule.Config> {

  /** Creates a OpenSearchProjectIndexScanRule. */
  protected OpenSearchProjectIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      // the ordinary variant
      final LogicalProject project = call.rel(0);
      final CalciteLogicalIndexScan scan = call.rel(1);
      apply(call, project, scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(RelOptRuleCall call, LogicalProject project, CalciteLogicalIndexScan scan) {
    final RelOptTable table = scan.getTable();
    requireNonNull(table.unwrap(OpenSearchIndex.class));

    final List<RexNode> projExprs = project.getProjects();
    final List<String> projNames = project.getRowType().getFieldNames();
    final int oldFieldCount = scan.getRowType().getFieldCount();

    final Set<String> usedNames = new HashSet<>(scan.osIndex.getFieldTypes().keySet());
    usedNames.addAll(scan.getPushDownContext().getDerivedScriptsByName().keySet());
    final Map<Integer, String> newDerivedAliases = new LinkedHashMap<>();
    // Collect which index in the scan is already derived field
    final BitSet derivedIndexSet = computeDerivedIndexSet(scan);
    final SelectedColumns selected = new SelectedColumns();
    final BitSet seenOldIndex = new BitSet(oldFieldCount);

    for (int i = 0; i < projExprs.size(); i++) {
      final RexNode projExpr = projExprs.get(i);
      if (isPushableNewDerived(projExpr, derivedIndexSet, scan)) {
        final String uniquifiedAlias =
            SqlValidatorUtil.uniquify(projNames.get(i), usedNames, SqlValidatorUtil.EXPR_SUGGESTER);
        newDerivedAliases.put(i, uniquifiedAlias);
        selected.add(SelectedColumn.derivedNew(i));
      } else {
        projExpr.accept(
            new RexVisitorImpl<Void>(true) {
              @Override
              public Void visitInputRef(RexInputRef inputRef) {
                final int oldIdx = inputRef.getIndex();
                if (!seenOldIndex.get(oldIdx)) {
                  seenOldIndex.set(oldIdx);
                  if (derivedIndexSet.get(oldIdx)) {
                    selected.add(SelectedColumn.derivedExisting(oldIdx));
                  } else {
                    selected.add(SelectedColumn.physical(oldIdx));
                  }
                }
                return null;
              }
            });
      }
    }

    /*
     * If there is no mapping to be projected or new derived field to be pushed,
     * or the pushed mapping is identical to current scan schema,
     * no need to push down project.
     */
    if (selected.isEmpty() || selected.isIdentity(oldFieldCount)) {
      return;
    }

    final CalciteLogicalIndexScan newScan =
        scan.pushDownProject(selected, project, newDerivedAliases);
    if (newScan == null) {
      return;
    }

    final Mapping oldIdxToNewPos =
        Mappings.create(MappingType.PARTIAL_FUNCTION, oldFieldCount, selected.size());
    final Map<Integer, Integer> projIdxToNewPos = new HashMap<>();

    for (int newPos = 0; newPos < selected.size(); newPos++) {
      final SelectedColumn selectedItem = selected.get(newPos);
      switch (selectedItem.getKind()) {
        case PHYSICAL:
        case DERIVED_EXISTING:
          oldIdxToNewPos.set(selectedItem.getOldIdx(), newPos);
          break;
        case DERIVED_NEW:
          projIdxToNewPos.put(selectedItem.getProjIdx(), newPos);
          break;
      }
    }

    final List<RexNode> newExprs = new ArrayList<>(projIdxToNewPos.size());
    for (int i = 0; i < projExprs.size(); i++) {
      if (projIdxToNewPos.containsKey(i)) {
        final int pos = projIdxToNewPos.get(i);
        newExprs.add(call.builder().getRexBuilder().makeInputRef(projExprs.get(i).getType(), pos));
      } else {
        newExprs.add(RexUtil.apply(oldIdxToNewPos, projExprs.get(i)));
      }
    }

    if (RexUtil.isIdentity(newExprs, newScan.getRowType())) {
      call.transformTo(newScan);
    } else {
      call.transformTo(
          call.builder()
              .push(newScan)
              .project(newExprs, project.getRowType().getFieldNames())
              .build());
    }
  }

  static final class SelectedColumns extends ArrayList<SelectedColumn> {
    private boolean isSequential = true;
    private Integer current = 0;

    @Override
    public boolean add(SelectedColumn item) {
      if (isSequential
          && item.getKind() == Kind.PHYSICAL
          && !Objects.equals(item.getOldIdx(), current++)) {
        isSequential = false;
      }
      return super.add(item);
    }

    public boolean isIdentity(Integer size) {
      return isSequential && size == size();
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    /** Config that matches Project on OpenSearchProjectIndexScanRule. */
    Config DEFAULT =
        ImmutableOpenSearchProjectIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalProject.class)
                        .oneInput(b1 -> b1.operand(CalciteLogicalIndexScan.class).noInputs()));

    @Override
    default OpenSearchProjectIndexScanRule toRule() {
      return new OpenSearchProjectIndexScanRule(this);
    }
  }

  private BitSet computeDerivedIndexSet(CalciteLogicalIndexScan scan) {
    final BitSet bitSet = new BitSet(scan.getRowType().getFieldCount());

    final List<String> names = scan.getRowType().getFieldNames();
    for (int i = 0; i < names.size(); i++) {
      final String name = names.get(i);
      if (scan.getPushDownContext().getDerivedScriptsByName().containsKey(name)) {
        bitSet.set(i);
      }
    }
    return bitSet;
  }

  private boolean isPushableNewDerived(
      RexNode node, BitSet derivedIndexSet, CalciteLogicalIndexScan scan) {
    boolean isValidExpr =
        node instanceof RexCall
            && !scan.getPushDownContext().isAggregatePushed()
            && !RexOver.containsOver(node)
            && !OpenSearchTypeFactory.findUDTType(node)
            && OpenSearchTypeFactory.isTypeSupportedForDerivedField(node.getType());
    if (!isValidExpr) {
      return false;
    }

    List<String> inputNames = scan.getRowType().getFieldNames();
    Set<String> indexFieldNames = scan.osIndex.getFieldTypes().keySet();
    final boolean[] canPushAsDerived = {false};
    // Chained derived field is not support. A new derived field can't rely on the input of existing derived field
    node.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            if (!derivedIndexSet.get(inputRef.getIndex())
                && indexFieldNames.contains(inputNames.get(inputRef.getIndex()))) {
              canPushAsDerived[0] = true;
            }
            return null;
          }
        });
    return canPushAsDerived[0];
  }
}

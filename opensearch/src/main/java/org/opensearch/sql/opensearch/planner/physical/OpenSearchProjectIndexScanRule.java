/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.BitSet;
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
import org.apache.calcite.rex.RexBiVisitorImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.opensearch.storage.scan.RexPermuteSelectedColumnShuttle;
import org.opensearch.sql.opensearch.storage.scan.RexPermuteSelectedColumnShuttle.SelectedColumnTargetMapping;
import org.opensearch.sql.opensearch.storage.scan.SelectedColumn;
import org.opensearch.sql.opensearch.storage.scan.SelectedColumn.Kind;

/** Planner rule that push a {@link LogicalProject} down to {@link CalciteLogicalIndexScan} */
@Value.Enclosing
public class OpenSearchProjectIndexScanRule extends RelRule<OpenSearchProjectIndexScanRule.Config> {

  private static final CanPushDerivedFieldBiVisitor canPushDerivedFieldBiVisitor =
      new CanPushDerivedFieldBiVisitor(true);
  private static final SelectedColumnBiVisitor selectedColumnBiVisitor =
      new SelectedColumnBiVisitor(true);

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

    final Set<String> usedNames = new HashSet<>(scan.osIndex.getAllFieldTypes().keySet());
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
        // Collect projected physical field and projected existing derived field
        projExpr.accept(
            selectedColumnBiVisitor, Triple.of(seenOldIndex, derivedIndexSet, selected));
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

    final SelectedColumnTargetMapping mapping =
        SelectedColumnTargetMapping.create(selected, scan.getRowType().getFieldCount());
    final List<RexNode> newExprs = RexPermuteSelectedColumnShuttle.of(mapping).visitList(projExprs);

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
          && (item.getKind() == Kind.PHYSICAL || item.getKind() == Kind.DERIVED_EXISTING)
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
            // After aggregation, it's not necessary to pushdown complex expressions on fewer rows
            && !scan.getPushDownContext().isAggregatePushed()
            && !RexOver.containsOver(node)
            && OpenSearchTypeFactory.isTypeSupportedForDerivedField(node.getType());
    if (!isValidExpr) {
      return false;
    }

    final boolean[] canPushAsDerivedState = {true, false}; // {canPush = true, seenInputRef = false}
    /*
     * Chained derived field is not support. A new derived field can't rely on the input of existing
     * derived field. If all of RexNode inputs are literals, we treat the RexNode result as literal.
     * And we don't push down such RexNode either.
     */
    node.accept(canPushDerivedFieldBiVisitor, Pair.of(derivedIndexSet, canPushAsDerivedState));
    return canPushAsDerivedState[0] && canPushAsDerivedState[1];
  }

  private static class SelectedColumnBiVisitor
      extends RexBiVisitorImpl<Void, Triple<BitSet, BitSet, SelectedColumns>> {
    protected SelectedColumnBiVisitor(boolean deep) {
      super(deep);
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef, Triple<BitSet, BitSet, SelectedColumns> args) {
      final BitSet seenOldIndex = args.getLeft();
      final BitSet derivedIndexSet = args.getMiddle();
      final SelectedColumns selected = args.getRight();
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
  }

  private static class CanPushDerivedFieldBiVisitor
      extends RexBiVisitorImpl<Void, Pair<BitSet, boolean[]>> {
    protected CanPushDerivedFieldBiVisitor(boolean deep) {
      super(deep);
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef, Pair<BitSet, boolean[]> args) {
      final BitSet derivedIndexSet = args.getLeft();
      // seenInputRef = true; Have seen one of RexNode input is input field
      args.getRight()[1] = true;
      if (derivedIndexSet.get(inputRef.getIndex())) {
        // canPush = false; Any existing derived field as input is not supported
        args.getRight()[0] = false;
      }
      return null;
    }
  }
}

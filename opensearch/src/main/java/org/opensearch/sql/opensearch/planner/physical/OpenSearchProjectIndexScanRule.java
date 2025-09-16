/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import static java.util.Objects.requireNonNull;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.planner.physical.RexPermuteIdentShuttle.Ident;
import org.opensearch.sql.opensearch.planner.physical.RexPermuteIdentShuttle.IdentRexVisitorImpl;
import org.opensearch.sql.opensearch.planner.physical.RexPermuteIdentShuttle.IdentTargetMapping;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

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

    // TODO: support script pushdown for project instead of only reference or field access
    // https://github.com/opensearch-project/sql/issues/3387
    final Set<Ident> selectedColumns = new LinkedHashSet<>();
    final IdentRexVisitorImpl identRexVisitor = new IdentRexVisitorImpl();
    final RexVisitorImpl<Void> visitor =
        new RexVisitorImpl<>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            Ident ident = new Ident(null, inputRef.getIndex(), null);
            selectedColumns.add(ident);
            return null;
          }

          @Override
          public Void visitFieldAccess(RexFieldAccess fieldAccess) {
            Ident prefix = fieldAccess.getReferenceExpr().accept(identRexVisitor);
            Ident ident =
                new Ident(prefix, fieldAccess.getField().getIndex(), fieldAccess.getField());
            selectedColumns.add(ident);
            return null;
          }
        };
    visitor.visitEach(project.getProjects());
    // Only do push down when an actual projection happens
    // TODO: fix this for FieldAccess
    List<Ident> identList = selectedColumns.stream().toList();
    if (!selectedColumns.isEmpty() && selectedColumns.size() != scan.getRowType().getFieldCount()) {
      IdentTargetMapping mapping =
          IdentTargetMapping.create(identList, scan.getRowType().getFieldCount());
      CalciteLogicalIndexScan newScan = scan.pushDownProject(identList, mapping);
      final List<RexNode> newProjectRexNodes =
          RexPermuteIdentShuttle.of(mapping, identRexVisitor).visitList(project.getProjects());

      if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
        call.transformTo(newScan);
      } else {
        call.transformTo(call.builder().push(newScan).project(newProjectRexNodes).build());
      }
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
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(OpenSearchIndexScanRule::noAggregatePushed)
                                    .noInputs()));

    @Override
    default OpenSearchProjectIndexScanRule toRule() {
      return new OpenSearchProjectIndexScanRule(this);
    }
  }
}

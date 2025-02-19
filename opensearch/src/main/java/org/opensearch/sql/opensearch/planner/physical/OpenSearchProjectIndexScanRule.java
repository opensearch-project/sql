/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteOpenSearchIndexScan;

/** Planner rule that push a {@link Project} down to {@link CalciteOpenSearchIndexScan} */
@Value.Enclosing
public class OpenSearchProjectIndexScanRule extends RelRule<OpenSearchProjectIndexScanRule.Config> {

  /** Creates a OpenSearchProjectIndexScanRule. */
  protected OpenSearchProjectIndexScanRule(Config config) {
    super(config);
  }

  protected static boolean test(CalciteOpenSearchIndexScan scan) {
    final RelOptTable table = scan.getTable();
    return table.unwrap(OpenSearchIndex.class) != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      // the ordinary variant
      final Project project = call.rel(0);
      final CalciteOpenSearchIndexScan scan = call.rel(1);
      apply(call, project, scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(RelOptRuleCall call, Project project, CalciteOpenSearchIndexScan scan) {
    final RelOptTable table = scan.getTable();
    requireNonNull(table.unwrap(OpenSearchIndex.class));

    final List<Integer> selectedColumns = new ArrayList<>();
    final RexVisitorImpl<Void> visitor =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            if (!selectedColumns.contains(inputRef.getIndex())) {
              selectedColumns.add(inputRef.getIndex());
            }
            return null;
          }
        };
    visitor.visitEach(project.getProjects());

    Mapping mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
    CalciteOpenSearchIndexScan newScan = scan.pushDownProject(selectedColumns);
    final List<RexNode> newProjectRexNodes = RexUtil.apply(mapping, project.getProjects());

    if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
      call.transformTo(newScan);
    } else {
      call.transformTo(call.builder().push(newScan).project(newProjectRexNodes).build());
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
                    b0.operand(Project.class)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteOpenSearchIndexScan.class)
                                    .predicate(OpenSearchProjectIndexScanRule::test)
                                    .noInputs()));

    @Override
    default OpenSearchProjectIndexScanRule toRule() {
      return new OpenSearchProjectIndexScanRule(this);
    }
  }
}

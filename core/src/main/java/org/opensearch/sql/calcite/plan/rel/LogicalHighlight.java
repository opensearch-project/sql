/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.HighlightExpression;

/**
 * Logical relational node representing a PPL {@code | highlight} command. Stores the highlight
 * arguments (terms to highlight or {@code *} for wildcard) and adds a {@code _highlight} column to
 * the output row type. An optimizer rule ({@code HighlightIndexScanRule}) pushes this node down
 * into the OpenSearch index scan.
 */
public class LogicalHighlight extends SingleRel {

  @Getter private final List<String> highlightArgs;
  private final RelDataType highlightRowType;

  protected LogicalHighlight(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<String> highlightArgs,
      RelDataType highlightRowType) {
    super(cluster, traitSet, input);
    this.highlightArgs = highlightArgs;
    this.highlightRowType = highlightRowType;
  }

  public static LogicalHighlight create(RelNode input, List<String> highlightArgs) {
    final RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);

    // Add _highlight column to the output row type so that downstream operators
    // (e.g. visitProject) can reference it before the optimizer rule fires.
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    RelDataTypeFactory.Builder schemaBuilder = typeFactory.builder();
    schemaBuilder.addAll(input.getRowType().getFieldList());
    if (!input.getRowType().getFieldNames().contains(HighlightExpression.HIGHLIGHT_FIELD)) {
      schemaBuilder.add(
          HighlightExpression.HIGHLIGHT_FIELD, typeFactory.createSqlType(SqlTypeName.ANY));
    }

    return new LogicalHighlight(cluster, traitSet, input, highlightArgs, schemaBuilder.build());
  }

  @Override
  protected RelDataType deriveRowType() {
    return highlightRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalHighlight(
        getCluster(), traitSet, sole(inputs), highlightArgs, highlightRowType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("highlightArgs", highlightArgs);
  }
}

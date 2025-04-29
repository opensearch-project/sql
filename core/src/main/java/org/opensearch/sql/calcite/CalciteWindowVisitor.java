/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC;

import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.OverCall;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;

public class CalciteWindowVisitor extends AbstractNodeVisitor<RexNode, CalcitePlanContext> {
  private final CalciteRexNodeVisitor rexNodeVisitor;
  private final CalciteAggCallVisitor aggVisitor;

  public CalciteWindowVisitor(
      CalciteRexNodeVisitor rexNodeVisitor, CalciteAggCallVisitor aggVisitor) {
    this.rexNodeVisitor = rexNodeVisitor;
    this.aggVisitor = aggVisitor;
  }

  public RexNode analyze(UnresolvedExpression unresolved, CalcitePlanContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public RexNode visitAlias(Alias node, CalcitePlanContext context) {
    RexNode overCallRexNode = analyze(node.getDelegated(), context);

    return context.relBuilder.alias(
        overCallRexNode, Strings.isEmpty(node.getAlias()) ? node.getName() : node.getAlias());
  }

  @Override
  public RexNode visitWindowFunction(WindowFunction node, CalcitePlanContext context) {
    AggCall aggCall = aggVisitor.analyze(node, context);

    OverCall overCall = aggCall.over();
    List<RexNode> partitionByFields =
        node.getPartitionByList().stream()
            .map(partitionKey -> rexNodeVisitor.analyze(partitionKey, context))
            .toList();
    List<RexNode> orderByFields =
        node.getSortList().stream()
            .map(
                pair -> {
                  RexNode sortField = rexNodeVisitor.analyze(pair.getRight(), context);
                  return pair.getLeft() == DEFAULT_DESC
                      ? context.relBuilder.desc(sortField)
                      : sortField;
                })
            .toList();
    return overCall
        .partitionBy(partitionByFields)
        .orderBy(orderByFields)
        .allowPartial(true)
        .nullWhenCountZero(false)
        .rowsUnbounded() // TODO: Add row bound if necessary for other window functions
        .toRex();
  }
}

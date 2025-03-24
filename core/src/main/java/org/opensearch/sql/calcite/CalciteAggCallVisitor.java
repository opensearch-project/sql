/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.AggregateUtils;

public class CalciteAggCallVisitor extends AbstractNodeVisitor<AggCall, CalcitePlanContext> {
  private final CalciteRexNodeVisitor rexNodeVisitor;

  public CalciteAggCallVisitor(CalciteRexNodeVisitor rexNodeVisitor) {
    this.rexNodeVisitor = rexNodeVisitor;
  }

  public AggCall analyze(UnresolvedExpression unresolved, CalcitePlanContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public AggCall visitAlias(Alias node, CalcitePlanContext context) {
    AggCall aggCall = analyze(node.getDelegated(), context);
    // Only OpenSearch SQL uses node.getAlias, OpenSearch PPL uses node.getName.
    return aggCall.as(Strings.isEmpty(node.getAlias()) ? node.getName() : node.getAlias());
  }

  @Override
  public AggCall visitAggregateFunction(AggregateFunction node, CalcitePlanContext context) {
    RexNode field = rexNodeVisitor.analyze(node.getField(), context);
    List<RexNode> argList = new ArrayList<>();
    for (UnresolvedExpression arg : node.getArgList()) {
      argList.add(rexNodeVisitor.analyze(arg, context));
    }
    return AggregateUtils.translate(node, field, context, argList);
  }
}

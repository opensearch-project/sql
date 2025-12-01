/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;

/**
 * Base class for converting specific portions of a RexNode tree by overriding the node types of
 * interest. This class implements the visitor pattern for RexNode traversal, providing default
 * implementations that return nodes unchanged. Subclasses can override specific visit methods to
 * transform particular node types while leaving others untouched.
 */
public class RexConverter implements RexVisitor<RexNode> {

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    return inputRef;
  }

  @Override
  public RexNode visitLocalRef(RexLocalRef localRef) {
    return localRef;
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal) {
    return literal;
  }

  @Override
  public RexNode visitCall(RexCall call) {
    List<RexNode> operands =
        call.getOperands().stream()
            .map(operand -> operand.accept(this))
            .collect(Collectors.toList());
    if (operands.equals(call.getOperands())) {
      return call;
    }
    return call.clone(call.getType(), operands);
  }

  @Override
  public RexNode visitOver(RexOver over) {
    List<RexNode> operands =
        over.getOperands().stream()
            .map(operand -> operand.accept(this))
            .collect(Collectors.toList());
    if (operands.equals(over.getOperands())) {
      return over;
    }
    return over.clone(over.getType(), operands);
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
    return correlVariable;
  }

  @Override
  public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    return dynamicParam;
  }

  @Override
  public RexNode visitRangeRef(RexRangeRef rangeRef) {
    return rangeRef;
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    RexNode expr = fieldAccess.getReferenceExpr().accept(this);
    if (expr == fieldAccess.getReferenceExpr()) {
      return fieldAccess;
    }
    throw new UnsupportedOperationException(
        "RexFieldAccess transformation not supported. Override visitFieldAccess() to handle this"
            + " case.");
  }

  @Override
  public RexNode visitSubQuery(RexSubQuery subQuery) {
    List<RexNode> operands =
        subQuery.getOperands().stream()
            .map(operand -> operand.accept(this))
            .collect(Collectors.toList());
    if (operands.equals(subQuery.getOperands())) {
      return subQuery;
    }
    return subQuery.clone(subQuery.getType(), operands);
  }

  @Override
  public RexNode visitTableInputRef(RexTableInputRef fieldRef) {
    return fieldRef;
  }

  @Override
  public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return fieldRef;
  }

  @Override
  public RexNode visitLambda(RexLambda lambda) {
    RexNode expr = lambda.getExpression().accept(this);
    if (expr == lambda.getExpression()) {
      return lambda;
    }
    throw new UnsupportedOperationException(
        "RexLambda transformation not supported. Override visitLambda() to handle this case.");
  }

  @Override
  public RexNode visitLambdaRef(RexLambdaRef lambdaRef) {
    return lambdaRef;
  }
}

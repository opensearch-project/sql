/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

public class LambdaUtils {
  public static Object transferLambdaOutputToTargetType(Object candidate, SqlTypeName targetType) {
    if (candidate instanceof BigDecimal) {
      BigDecimal bd = (BigDecimal) candidate;
      switch (targetType) {
        case INTEGER:
          return bd.intValue();
        case DOUBLE:
          return bd.doubleValue();
        case FLOAT:
          return bd.floatValue();
        default:
          return bd;
      }
    } else {
      return candidate;
    }
  }

  public static RelDataType inferReturnTypeFromLambda(
      RexLambda rexLambda, Map<String, RelDataType> filledTypes, RelDataTypeFactory typeFactory) {
    RexCall rexCall = (RexCall) rexLambda.getExpression();
    SqlReturnTypeInference returnInfer = rexCall.getOperator().getReturnTypeInference();
    List<RexNode> lambdaOperands = rexCall.getOperands();
    List<RexNode> filledOperands = new ArrayList<>();
    for (RexNode rexNode : lambdaOperands) {
      if (rexNode instanceof RexLambdaRef rexLambdaRef) {
        if (rexLambdaRef.getType().getSqlTypeName() == SqlTypeName.ANY) {
          filledOperands.add(
              new RexLambdaRef(
                  rexLambdaRef.getIndex(),
                  rexLambdaRef.getName(),
                  filledTypes.get(rexLambdaRef.getName())));
        } else {
          filledOperands.add(rexNode);
        }
      } else if (rexNode instanceof RexCall) {
        filledOperands.add(
            reInferReturnTypeForRexCallInsideLambda((RexCall) rexNode, filledTypes, typeFactory));
      } else {
        filledOperands.add(rexNode);
      }
    }
    return returnInfer.inferReturnType(
        new RexCallBinding(typeFactory, rexCall.getOperator(), filledOperands, List.of()));
  }

  public static RexCall reInferReturnTypeForRexCallInsideLambda(
      RexCall rexCall, Map<String, RelDataType> argTypes, RelDataTypeFactory typeFactory) {
    List<RexNode> filledOperands = new ArrayList<>();
    List<RexNode> rexCallOperands = rexCall.getOperands();
    for (RexNode rexNode : rexCallOperands) {
      if (rexNode instanceof RexLambdaRef rexLambdaRef) {
        filledOperands.add(
            new RexLambdaRef(
                rexLambdaRef.getIndex(),
                rexLambdaRef.getName(),
                argTypes.get(rexLambdaRef.getName())));
      } else if (rexNode instanceof RexCall) {
        filledOperands.add(
            reInferReturnTypeForRexCallInsideLambda((RexCall) rexNode, argTypes, typeFactory));
      } else {
        filledOperands.add(rexNode);
      }
    }
    RelDataType returnType =
        rexCall
            .getOperator()
            .inferReturnType(
                new RexCallBinding(typeFactory, rexCall.getOperator(), filledOperands, List.of()));
    return rexCall.clone(returnType, filledOperands);
  }
}

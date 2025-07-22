/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.WideningTypeRule;

public class CoercionUtils {
  public static @Nullable List<RexNode> castArguments(
      RexBuilder builder, PPLTypeChecker typeChecker, List<RexNode> arguments) {
    List<List<ExprType>> paramTypeCombinations = typeChecker.getParameterTypes();

    // TODO: var args?

    for (List<ExprType> paramTypes : paramTypeCombinations) {
      List<RexNode> castedArguments = castArguments(builder, paramTypes, arguments);
      if (castedArguments != null) {
        return castedArguments;
      }
    }
    return null;
  }

  /**
   * Casts the arguments to the types specified in paramTypes. Returns null if the number of
   * parameters does not match or if casting fails.
   */
  private static @Nullable List<RexNode> castArguments(
      RexBuilder builder, List<ExprType> paramTypes, List<RexNode> arguments) {
    if (paramTypes.size() != arguments.size()) {
      return null; // Skip if the number of parameters does not match
    }

    List<RexNode> castedArguments = new ArrayList<>();
    for (int i = 0; i < paramTypes.size(); i++) {
      ExprType toType = paramTypes.get(i);
      RexNode arg = arguments.get(i);

      RexNode castedArg = cast(builder, toType, arg);

      if (castedArg == null) {
        return null;
      }
      castedArguments.add(castedArg);
    }
    return castedArguments;
  }

  private static @Nullable RexNode cast(RexBuilder builder, ExprType targetType, RexNode arg) {
    // Implement the logic to check if fromType can be cast to toType
    // This could involve checking if the types are compatible, or if a cast is possible
    // For example, you might check if fromType is a subtype of toType, or if a conversion exists
    ExprType argType = OpenSearchTypeFactory.convertRelDataTypeToExprType(arg.getType());
    if (!argType.shouldCast(targetType)) {
      return arg;
    }

    // If the arg is string
    if (WideningTypeRule.distance(argType, targetType) != WideningTypeRule.IMPOSSIBLE_WIDENING) {
      return builder.makeCast(OpenSearchTypeFactory.convertExprTypeToRelDataType(targetType), arg);
    }
    return null;
  }
}

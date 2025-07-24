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
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.WideningTypeRule;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class CoercionUtils {

  /**
   * Casts the arguments to the types specified in the typeChecker. Returns null if no combination
   * of parameter types matches the arguments or if casting fails.
   *
   * @param builder RexBuilder to create casts
   * @param typeChecker PPLTypeChecker that provides the parameter types
   * @param arguments List of RexNode arguments to be cast
   * @return List of cast RexNode arguments or null if casting fails
   */
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
   * Widen the arguments to the widest type found among them. If no widest type can be determined,
   * returns null.
   *
   * @param builder RexBuilder to create casts
   * @param arguments List of RexNode arguments to be widened
   * @return List of widened RexNode arguments or null if no widest type can be determined
   */
  public static @Nullable List<RexNode> widenArguments(
      RexBuilder builder, List<RexNode> arguments) {
    // TODO: Add test on e.g. IP
    ExprType widestType = findWidestType(arguments);
    if (widestType == null) {
      return null; // No widest type found, return null
    }
    return arguments.stream().map(arg -> cast(builder, widestType, arg)).toList();
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

  /**
   * Finds the widest type among the given arguments. The widest type is determined by applying the
   * widening type rule to each pair of types in the arguments.
   *
   * @param arguments List of RexNode arguments to find the widest type from
   * @return the widest ExprType if found, otherwise null
   */
  private static @Nullable ExprType findWidestType(List<RexNode> arguments) {
    if (arguments.isEmpty()) {
      return null; // No arguments to process
    }
    ExprType widestType =
        OpenSearchTypeFactory.convertRelDataTypeToExprType(arguments.getFirst().getType());
    if (arguments.size() == 1) {
      return widestType;
    }

    // Iterate pairwise through the arguments and find the widest type
    for (int i = 1; i < arguments.size(); i++) {
      var type = OpenSearchTypeFactory.convertRelDataTypeToExprType(arguments.get(i).getType());
      try {
        if (areDateAndTime(widestType, type)) {
          // If one is date and the other is time, we consider timestamp as the widest type
          widestType = ExprCoreType.TIMESTAMP;
        } else {
          widestType = WideningTypeRule.max(widestType, type);
        }
      } catch (ExpressionEvaluationException e) {
        // the two types are not compatible, return null
        return null;
      }
    }
    return widestType;
  }

  private static boolean areDateAndTime(ExprType type1, ExprType type2) {
    return (type1 == ExprCoreType.DATE && type2 == ExprCoreType.TIME)
        || (type1 == ExprCoreType.TIME && type2 == ExprCoreType.DATE);
  }
}

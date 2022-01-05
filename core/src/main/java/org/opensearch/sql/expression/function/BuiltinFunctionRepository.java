/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.ast.expression.Cast.getCastFunctionName;
import static org.opensearch.sql.ast.expression.Cast.isCastFunction;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;

/**
 * Builtin Function Repository.
 */
@RequiredArgsConstructor
public class BuiltinFunctionRepository {
  private final Map<FunctionName, FunctionResolver> functionResolverMap;

  /**
   * Register {@link FunctionResolver} to the Builtin Function Repository.
   *
   * @param resolver {@link FunctionResolver} to be registered
   */
  public void register(FunctionResolver resolver) {
    functionResolverMap.put(resolver.getFunctionName(), resolver);
  }

  /**
   * Compile FunctionExpression.
   */
  public FunctionImplementation compile(FunctionName functionName, List<Expression> expressions) {
    FunctionBuilder resolvedFunctionBuilder = resolve(new FunctionSignature(functionName,
        expressions.stream().map(expression -> expression.type()).collect(Collectors.toList())));
    return resolvedFunctionBuilder.apply(expressions);
  }

  /**
   * Resolve the {@link FunctionBuilder} in Builtin Function Repository.
   *
   * @param functionSignature {@link FunctionSignature}
   * @return Original function builder if it's a cast function or all arguments have expected types.
   *         Otherwise wrap its arguments by cast function as needed.
   */
  public FunctionBuilder resolve(FunctionSignature functionSignature) {
    FunctionName functionName = functionSignature.getFunctionName();
    if (functionResolverMap.containsKey(functionName)) {
      Pair<FunctionSignature, FunctionBuilder> resolvedSignature =
          functionResolverMap.get(functionName).resolve(functionSignature);

      List<ExprType> sourceTypes = functionSignature.getParamTypeList();
      List<ExprType> targetTypes = resolvedSignature.getKey().getParamTypeList();
      FunctionBuilder funcBuilder = resolvedSignature.getValue();
      if (isCastFunction(functionName) || sourceTypes.equals(targetTypes)) {
        return funcBuilder;
      }
      return castArguments(sourceTypes, targetTypes, funcBuilder);
    } else {
      throw new ExpressionEvaluationException(
          String.format("unsupported function name: %s", functionName.getFunctionName()));
    }
  }

  /**
   * Wrap resolved function builder's arguments by cast function to cast input expression value
   * to value of target type at runtime. For example, suppose unresolved signature is
   * equal(BOOL,STRING) and its resolved function builder is F with signature equal(BOOL,BOOL).
   * In this case, wrap F and return equal(BOOL, cast_to_bool(STRING)).
   */
  private FunctionBuilder castArguments(List<ExprType> sourceTypes,
                                        List<ExprType> targetTypes,
                                        FunctionBuilder funcBuilder) {
    return arguments -> {
      List<Expression> argsCasted = new ArrayList<>();
      for (int i = 0; i < arguments.size(); i++) {
        Expression arg = arguments.get(i);
        ExprType sourceType = sourceTypes.get(i);
        ExprType targetType = targetTypes.get(i);

        if (isCastRequired(sourceType, targetType)) {
          argsCasted.add(cast(arg, targetType));
        } else {
          argsCasted.add(arg);
        }
      }
      return funcBuilder.apply(argsCasted);
    };
  }

  private boolean isCastRequired(ExprType sourceType, ExprType targetType) {
    // TODO: Remove this special case after fixing all failed UTs
    if (ExprCoreType.numberTypes().contains(sourceType)
        && ExprCoreType.numberTypes().contains(targetType)) {
      return false;
    }
    return sourceType.shouldCast(targetType);
  }

  /**
   * Cast Expression to target ExprType.
   *
   * @param arg expression
   * @param targetType target ExprType
   * @return Expression
   */
  public Expression cast(Expression arg, ExprType targetType) {
    FunctionName castFunctionName = getCastFunctionName(targetType);
    if (castFunctionName == null) {
      throw new ExpressionEvaluationException(StringUtils.format(
          "Type conversion to type %s is not supported", targetType));
    }
    return (Expression) compile(castFunctionName, ImmutableList.of(arg));
  }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
   * Wrap arguments by cast function as needed. Return original function builder
   * if it's already a cast function or all arguments have expected types.
   *
   * @param functionSignature {@link FunctionSignature}
   * @return {@link FunctionBuilder}
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
   * Wrap resolved function builder's arguments by cast function which is to cast expression value
   * to value of target type at runtime.
   * For example, suppose unresolved signature is equal(BOOL,STRING), and resolved function builder
   * is F with signature equal(BOOL,BOOL). In this case, wrap F and return
   * equal(BOOL, cast_to_bool(STRING)).
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

        if (isCastNotNeeded(sourceType, targetType)) {
          argsCasted.add(arg);
        } else {
          argsCasted.add(cast(arg, targetType));
        }
      }
      return funcBuilder.apply(argsCasted);
    };
  }

  /**
   * 1) Source and target type are the same.
   * 2) Casting from number to another number is built-in supported in JDK ???
   */
  private boolean isCastNotNeeded(ExprType sourceType, ExprType targetType) {
    if (sourceType.equals(targetType)) {
      return true;
    }

    return ExprCoreType.numberTypes().contains(sourceType)
        && ExprCoreType.numberTypes().contains(targetType);
  }

  private Expression cast(Expression arg, ExprType targetType) {
    FunctionName castFunctionName = getCastFunctionName(targetType);
    if (castFunctionName == null) {
      throw new ExpressionEvaluationException(StringUtils.format(
          "Type conversion to type %s is not supported", targetType));
    }
    return (Expression) compile(castFunctionName, ImmutableList.of(arg));
  }

}

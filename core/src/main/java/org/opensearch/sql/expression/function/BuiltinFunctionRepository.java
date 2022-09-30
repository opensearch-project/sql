/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.ast.expression.Cast.getCastFunctionName;
import static org.opensearch.sql.ast.expression.Cast.isCastFunction;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
 * Repository registers catalog specific functions under catalog specific namespace and
 * universal functions under default namespace. Catalog Specific Namespace carries their own
 * namespace.
 *
 */
@RequiredArgsConstructor
public class BuiltinFunctionRepository {

  public static final String DEFAULT_NAMESPACE = "default";

  private final Map<String, Map<FunctionName, FunctionResolver>> namespaceFunctionResolverMap;


  /**
   * Register {@link DefaultFunctionResolver} to the Builtin Function Repository
   * under default namespace.
   *
   * @param resolver {@link DefaultFunctionResolver} to be registered
   */
  public void register(FunctionResolver resolver) {
    register(DEFAULT_NAMESPACE, resolver);
  }

  /**
   * Register {@link DefaultFunctionResolver} to the Builtin Function Repository with
   * specified namespace.
   *
   * @param resolver {@link DefaultFunctionResolver} to be registered
   */
  public void register(String namespace, FunctionResolver resolver) {
    Map<FunctionName, FunctionResolver> functionResolverMap;
    if (!namespaceFunctionResolverMap.containsKey(namespace)) {
      functionResolverMap = new HashMap<>();
      namespaceFunctionResolverMap.put(namespace, functionResolverMap);
    }
    namespaceFunctionResolverMap.get(namespace).put(resolver.getFunctionName(), resolver);
  }


  /**
   * Compile FunctionExpression under default namespace.
   *
   */
  public FunctionImplementation compile(FunctionName functionName, List<Expression> expressions) {
    return compile(DEFAULT_NAMESPACE, functionName, expressions);
  }


  /**
   * Compile FunctionExpression within given namespace.
   * Checks for default namespace first and then tries to compile from given namespace.
   */
  public FunctionImplementation compile(String namespace, FunctionName functionName,
                                        List<Expression> expressions) {
    List<String> namespaceList = new ArrayList<>(List.of(DEFAULT_NAMESPACE));
    if (!namespace.equals(DEFAULT_NAMESPACE)) {
      namespaceList.add(namespace);
    }
    FunctionBuilder resolvedFunctionBuilder = resolve(namespaceList,
        new FunctionSignature(functionName, expressions
            .stream().map(expression -> expression.type()).collect(Collectors.toList())));
    return resolvedFunctionBuilder.apply(expressions);
  }

  /**
   * Resolve the {@link FunctionBuilder} in
   * repository under a list of namespaces.
   * Returns the First FunctionBuilder found.
   * So list of namespaces is also the priority of namespaces.
   *
   * @param functionSignature {@link FunctionSignature} functionsignature.
   *
   * @return Original function builder if it's a cast function or all arguments have expected types
   *         or other wise wrap its arguments by cast function as needed.
   */
  public FunctionBuilder resolve(List<String> namespaces, FunctionSignature functionSignature) {
    FunctionName functionName = functionSignature.getFunctionName();
    FunctionBuilder result = null;
    for (String namespace : namespaces) {
      if (namespaceFunctionResolverMap.containsKey(namespace)
          && namespaceFunctionResolverMap.get(namespace).containsKey(functionName)) {
        result = getFunctionBuilder(functionSignature, functionName,
            namespaceFunctionResolverMap.get(namespace));
        break;
      }
    }
    if (result == null) {
      throw new ExpressionEvaluationException(
          String.format("unsupported function name: %s", functionName.getFunctionName()));
    } else {
      return result;
    }
  }

  private FunctionBuilder getFunctionBuilder(FunctionSignature functionSignature,
                       FunctionName functionName,
                       Map<FunctionName, FunctionResolver> functionResolverMap) {
    Pair<FunctionSignature, FunctionBuilder> resolvedSignature =
        functionResolverMap.get(functionName).resolve(functionSignature);

    List<ExprType> sourceTypes = functionSignature.getParamTypeList();
    List<ExprType> targetTypes = resolvedSignature.getKey().getParamTypeList();
    FunctionBuilder funcBuilder = resolvedSignature.getValue();
    if (isCastFunction(functionName) || sourceTypes.equals(targetTypes)) {
      return funcBuilder;
    }
    return castArguments(sourceTypes, targetTypes, funcBuilder);
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

  private Expression cast(Expression arg, ExprType targetType) {
    FunctionName castFunctionName = getCastFunctionName(targetType);
    if (castFunctionName == null) {
      throw new ExpressionEvaluationException(StringUtils.format(
          "Type conversion to type %s is not supported", targetType));
    }
    return (Expression) compile(castFunctionName, ImmutableList.of(arg));
  }

}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.function;

import static org.opensearch.sql.ast.expression.Cast.getCastFunctionName;
import static org.opensearch.sql.ast.expression.Cast.isCastFunction;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.aggregation.AggregatorFunction;
import org.opensearch.sql.expression.datetime.DateTimeFunction;
import org.opensearch.sql.expression.datetime.IntervalClause;
import org.opensearch.sql.expression.operator.arthmetic.ArithmeticFunction;
import org.opensearch.sql.expression.operator.arthmetic.MathematicalFunction;
import org.opensearch.sql.expression.operator.convert.TypeCastOperator;
import org.opensearch.sql.expression.operator.predicate.BinaryPredicateOperator;
import org.opensearch.sql.expression.operator.predicate.UnaryPredicateOperator;
import org.opensearch.sql.expression.system.SystemFunctions;
import org.opensearch.sql.expression.text.TextFunction;
import org.opensearch.sql.expression.window.WindowFunctions;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Builtin Function Repository.
 * Repository registers datasource specific functions under datasource namespace and
 * universal functions under default namespace.
 *
 */
public class BuiltinFunctionRepository {

  private final Map<FunctionName, FunctionResolver> functionResolverMap;

  /** The singleton instance. */
  private static BuiltinFunctionRepository instance;

  /**
   * Construct a function repository with the given function registered. This is only used in test.
   *
   * @param functionResolverMap function supported
   */
  @VisibleForTesting
  BuiltinFunctionRepository(Map<FunctionName, FunctionResolver> functionResolverMap) {
    this.functionResolverMap = functionResolverMap;
  }

  /**
   * Get singleton instance of the function repository. Initialize it with all built-in functions
   * for the first time in synchronized way.
   *
   * @return singleton instance
   */
  public static synchronized BuiltinFunctionRepository getInstance() {
    if (instance == null) {
      instance = new BuiltinFunctionRepository(new HashMap<>());

      // Register all built-in functions
      ArithmeticFunction.register(instance);
      BinaryPredicateOperator.register(instance);
      MathematicalFunction.register(instance);
      UnaryPredicateOperator.register(instance);
      AggregatorFunction.register(instance);
      DateTimeFunction.register(instance);
      IntervalClause.register(instance);
      WindowFunctions.register(instance);
      TextFunction.register(instance);
      TypeCastOperator.register(instance);
      SystemFunctions.register(instance);
      OpenSearchFunctions.register(instance);
    }
    return instance;
  }

  /**
   * Register {@link DefaultFunctionResolver} to the Builtin Function Repository.
   *
   * @param resolver {@link DefaultFunctionResolver} to be registered
   */
  public void register(FunctionResolver resolver) {
    functionResolverMap.put(resolver.getFunctionName(), resolver);
  }

  /**
   * Compile FunctionExpression using core function resolver.
   *
   */
  public FunctionImplementation compile(FunctionProperties functionProperties,
                                        FunctionName functionName, List<Expression> expressions) {
    return compile(functionProperties, Collections.emptyList(), functionName, expressions);
  }


  /**
   * Compile FunctionExpression within {@link StorageEngine} provided {@link FunctionResolver}.
   */
  public FunctionImplementation compile(FunctionProperties functionProperties,
                                        Collection<FunctionResolver> dataSourceFunctionResolver,
                                        FunctionName functionName,
                                        List<Expression> expressions) {
    FunctionBuilder resolvedFunctionBuilder =
        resolve(
            dataSourceFunctionResolver,
            new FunctionSignature(
                functionName,
                expressions.stream().map(Expression::type).collect(Collectors.toList())));
    return resolvedFunctionBuilder.apply(functionProperties, expressions);
  }

  /**
   * Resolve the {@link FunctionBuilder} in repository under a list of namespaces. Returns the First
   * FunctionBuilder found. So list of namespaces is also the priority of namespaces.
   *
   * @param functionSignature {@link FunctionSignature} functionsignature.
   * @return Original function builder if it's a cast function or all arguments have expected types
   *     or otherwise wrap its arguments by cast function as needed.
   */
  @VisibleForTesting
  public FunctionBuilder resolve(
      Collection<FunctionResolver> dataSourceFunctionResolver,
      FunctionSignature functionSignature) {
    Map<FunctionName, FunctionResolver> dataSourceFunctionMap = dataSourceFunctionResolver.stream()
        .collect(Collectors.toMap(FunctionResolver::getFunctionName, t -> t));

    // first, resolve in datasource provide function resolver.
    // second, resolve in builtin function resolver.
    return resolve(functionSignature, dataSourceFunctionMap)
        .or(() -> resolve(functionSignature, functionResolverMap))
        .orElseThrow(
            () ->
                new ExpressionEvaluationException(
                    String.format(
                        "unsupported function name: %s", functionSignature.getFunctionName())));
  }

  private Optional<FunctionBuilder> resolve(
      FunctionSignature functionSignature,
      Map<FunctionName, FunctionResolver> functionResolverMap) {
    FunctionName functionName = functionSignature.getFunctionName();
    if (functionResolverMap.containsKey(functionName)) {
      Pair<FunctionSignature, FunctionBuilder> resolvedSignature =
          functionResolverMap.get(functionName).resolve(functionSignature);

      List<ExprType> sourceTypes = functionSignature.getParamTypeList();
      List<ExprType> targetTypes = resolvedSignature.getKey().getParamTypeList();
      FunctionBuilder funcBuilder = resolvedSignature.getValue();
      if (isCastFunction(functionName)
          || FunctionSignature.isVarArgFunction(targetTypes)
          || sourceTypes.equals(targetTypes)) {
        return Optional.of(funcBuilder);
      }
      return Optional.of(castArguments(sourceTypes, targetTypes, funcBuilder));
    } else {
      return Optional.empty();
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
    return (fp, arguments) -> {
      List<Expression> argsCasted = new ArrayList<>();
      for (int i = 0; i < arguments.size(); i++) {
        Expression arg = arguments.get(i);
        ExprType sourceType = sourceTypes.get(i);
        ExprType targetType = targetTypes.get(i);

        if (isCastRequired(sourceType, targetType)) {
          argsCasted.add(cast(arg, targetType).apply(fp));
        } else {
          argsCasted.add(arg);
        }
      }
      return funcBuilder.apply(fp, argsCasted);
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

  private Function<FunctionProperties, Expression> cast(Expression arg, ExprType targetType) {
    FunctionName castFunctionName = getCastFunctionName(targetType);
    if (castFunctionName == null) {
      throw new ExpressionEvaluationException(StringUtils.format(
          "Type conversion to type %s is not supported", targetType));
    }
    return functionProperties -> (Expression) compile(functionProperties,
        castFunctionName, List.of(arg));
  }
}

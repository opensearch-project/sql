/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionDSL;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.SerializableFunction;

/**
 * The definition of unary predicate function not, Accepts one Boolean value and produces a Boolean.
 */
@UtilityClass
public class UnaryPredicateOperator {
  /** Register Unary Predicate Function. */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(not());
    repository.register(isNotNull());
    repository.register(ifNull());
    repository.register(nullIf());
    repository.register(isNull(BuiltinFunctionName.IS_NULL));
    repository.register(isNull(BuiltinFunctionName.ISNULL));
    repository.register(ifFunction());
  }

  private static DefaultFunctionResolver not() {
    return FunctionDSL.define(
        BuiltinFunctionName.NOT.getName(),
        FunctionDSL.impl(UnaryPredicateOperator::not, BOOLEAN, BOOLEAN));
  }

  /**
   * The not logic.
   *
   * <table>
   *     <tr>
   *         <th>A</th>
   *         <th>NOT A</th>
   *     </tr>
   *     <tr>
   *         <th>TRUE</th>
   *         <th>FALSE</th>
   *     </tr>
   *     <tr>
   *         <th>FALSE</th>
   *         <th>TRUE</th>
   *     </tr>
   *     <tr>
   *         <th>NULL</th>
   *         <th>NULL</th>
   *     </tr>
   *     <tr>
   *         <th>MISSING</th>
   *         <th>MISSING</th>
   *     </tr>
   * </table>
   */
  public ExprValue not(ExprValue v) {
    if (v.isMissing() || v.isNull()) {
      return v;
    } else {
      return ExprBooleanValue.of(!v.booleanValue());
    }
  }

  private static DefaultFunctionResolver isNull(BuiltinFunctionName funcName) {
    return FunctionDSL.define(
        funcName.getName(),
        Arrays.stream(ExprCoreType.values())
            .map(type -> FunctionDSL.impl((v) -> ExprBooleanValue.of(v.isNull()), BOOLEAN, type))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver isNotNull() {
    return FunctionDSL.define(
        BuiltinFunctionName.IS_NOT_NULL.getName(),
        Arrays.stream(ExprCoreType.values())
            .map(type -> FunctionDSL.impl((v) -> ExprBooleanValue.of(!v.isNull()), BOOLEAN, type))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver ifFunction() {
    FunctionName functionName = BuiltinFunctionName.IF.getName();
    List<ExprCoreType> typeList = ExprCoreType.coreTypes();

    List<
            SerializableFunction<
                FunctionName,
                org.apache.commons.lang3.tuple.Pair<FunctionSignature, FunctionBuilder>>>
        functionsOne =
            typeList.stream()
                .map(v -> impl((UnaryPredicateOperator::exprIf), v, BOOLEAN, v, v))
                .collect(Collectors.toList());

    DefaultFunctionResolver functionResolver = FunctionDSL.define(functionName, functionsOne);
    return functionResolver;
  }

  private static DefaultFunctionResolver ifNull() {
    FunctionName functionName = BuiltinFunctionName.IFNULL.getName();
    List<ExprCoreType> typeList = ExprCoreType.coreTypes();

    List<
            SerializableFunction<
                FunctionName,
                org.apache.commons.lang3.tuple.Pair<FunctionSignature, FunctionBuilder>>>
        functionsOne =
            typeList.stream()
                .map(v -> impl((UnaryPredicateOperator::exprIfNull), v, v, v))
                .collect(Collectors.toList());

    DefaultFunctionResolver functionResolver = FunctionDSL.define(functionName, functionsOne);
    return functionResolver;
  }

  private static DefaultFunctionResolver nullIf() {
    FunctionName functionName = BuiltinFunctionName.NULLIF.getName();
    List<ExprCoreType> typeList = ExprCoreType.coreTypes();

    DefaultFunctionResolver functionResolver =
        FunctionDSL.define(
            functionName,
            typeList.stream()
                .map(v -> impl((UnaryPredicateOperator::exprNullIf), v, v, v))
                .collect(Collectors.toList()));
    return functionResolver;
  }

  /**
   * v2 if v1 is null.
   *
   * @param v1 varable 1
   * @param v2 varable 2
   * @return v2 if v1 is null
   */
  public static ExprValue exprIfNull(ExprValue v1, ExprValue v2) {
    return (v1.isNull() || v1.isMissing()) ? v2 : v1;
  }

  /**
   * return null if v1 equls to v2.
   *
   * @param v1 varable 1
   * @param v2 varable 2
   * @return null if v1 equls to v2
   */
  public static ExprValue exprNullIf(ExprValue v1, ExprValue v2) {
    return v1.equals(v2) ? LITERAL_NULL : v1;
  }

  public static ExprValue exprIf(ExprValue v1, ExprValue v2, ExprValue v3) {
    return !v1.isNull() && !v1.isMissing() && LITERAL_TRUE.equals(v1) ? v2 : v3;
  }
}

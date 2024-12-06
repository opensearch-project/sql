/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.DefaultFunctionResolver.DefaultFunctionResolverBuilder;

/** Function Define Utility. */
@UtilityClass
public class FunctionDSL {
  /**
   * Define overloaded function with implementation.
   *
   * @param functionName function name.
   * @param functions a list of function implementation.
   * @return FunctionResolver.
   */
  public static DefaultFunctionResolver define(
      FunctionName functionName,
      SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>... functions) {
    return define(functionName, List.of(functions));
  }

  /**
   * Define overloaded function with implementation.
   *
   * @param functionName function name.
   * @param functions a list of function implementation.
   * @return FunctionResolver.
   */
  public static DefaultFunctionResolver define(
      FunctionName functionName,
      List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>>
          functions) {

    DefaultFunctionResolverBuilder builder = DefaultFunctionResolver.builder();
    builder.functionName(functionName);
    for (Function<FunctionName, Pair<FunctionSignature, FunctionBuilder>> func : functions) {
      Pair<FunctionSignature, FunctionBuilder> functionBuilder = func.apply(functionName);
      builder.functionBundle(functionBuilder.getKey(), functionBuilder.getValue());
    }
    return builder.build();
  }

  /**
   * Implementation of no args function that uses FunctionProperties.
   *
   * @param function {@link ExprValue} based no args function.
   * @param returnType function return type.
   * @return no args function implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
      implWithProperties(
          SerializableFunction<FunctionProperties, ExprValue> function, ExprType returnType) {
    return functionName -> {
      FunctionSignature functionSignature = new FunctionSignature(functionName, emptyList());
      FunctionBuilder functionBuilder =
          (functionProperties, arguments) ->
              new FunctionExpression(functionName, emptyList()) {
                @Override
                public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                  return function.apply(functionProperties);
                }

                @Override
                public ExprType type() {
                  return returnType;
                }

                @Override
                public String toString() {
                  return String.format("%s()", functionName);
                }
              };
      return Pair.of(functionSignature, functionBuilder);
    };
  }

  /**
   * Implementation of a function that takes one argument, returns a value, and requires
   * FunctionProperties to complete.
   *
   * @param function {@link ExprValue} based unary function.
   * @param returnType return type.
   * @param argsType argument type.
   * @return Unary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
      implWithProperties(
          SerializableBiFunction<FunctionProperties, ExprValue, ExprValue> function,
          ExprType returnType,
          ExprType argsType) {

    return functionName -> {
      FunctionSignature functionSignature =
          new FunctionSignature(functionName, singletonList(argsType));
      FunctionBuilder functionBuilder =
          (functionProperties, arguments) ->
              new FunctionExpression(functionName, arguments) {
                @Override
                public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                  ExprValue value = arguments.get(0).valueOf(valueEnv);
                  return function.apply(functionProperties, value);
                }

                @Override
                public ExprType type() {
                  return returnType;
                }

                @Override
                public String toString() {
                  return String.format(
                      "%s(%s)",
                      functionName,
                      arguments.stream().map(Object::toString).collect(Collectors.joining(", ")));
                }
              };
      return Pair.of(functionSignature, functionBuilder);
    };
  }

  /**
   * Implementation of a function that takes two arguments, returns a value, and requires
   * FunctionProperties to complete.
   *
   * @param function {@link ExprValue} based Binary function.
   * @param returnType return type.
   * @param args1Type first argument type.
   * @param args2Type second argument type.
   * @return Binary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
      implWithProperties(
          SerializableTriFunction<FunctionProperties, ExprValue, ExprValue, ExprValue> function,
          ExprType returnType,
          ExprType args1Type,
          ExprType args2Type) {

    return functionName -> {
      FunctionSignature functionSignature =
          new FunctionSignature(functionName, Arrays.asList(args1Type, args2Type));
      FunctionBuilder functionBuilder =
          (functionProperties, arguments) ->
              new FunctionExpression(functionName, arguments) {
                @Override
                public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                  ExprValue arg1 = arguments.get(0).valueOf(valueEnv);
                  ExprValue arg2 = arguments.get(1).valueOf(valueEnv);
                  return function.apply(functionProperties, arg1, arg2);
                }

                @Override
                public ExprType type() {
                  return returnType;
                }

                @Override
                public String toString() {
                  return String.format(
                      "%s(%s)",
                      functionName,
                      arguments.stream().map(Object::toString).collect(Collectors.joining(", ")));
                }
              };
      return Pair.of(functionSignature, functionBuilder);
    };
  }

  /**
   * Implementation of a function that takes three arguments, returns a value, and requires
   * FunctionProperties to complete.
   *
   * @param function {@link ExprValue} based Binary function.
   * @param returnType return type.
   * @param args1Type first argument type.
   * @param args2Type second argument type.
   * @param args3Type third argument type.
   * @return Binary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>
      implWithProperties(
          SerializableQuadFunction<FunctionProperties, ExprValue, ExprValue, ExprValue, ExprValue>
              function,
          ExprType returnType,
          ExprType args1Type,
          ExprType args2Type,
          ExprType args3Type) {

    return functionName -> {
      FunctionSignature functionSignature =
          new FunctionSignature(functionName, Arrays.asList(args1Type, args2Type, args3Type));
      FunctionBuilder functionBuilder =
          (functionProperties, arguments) ->
              new FunctionExpression(functionName, arguments) {
                @Override
                public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                  ExprValue arg1 = arguments.get(0).valueOf(valueEnv);
                  ExprValue arg2 = arguments.get(1).valueOf(valueEnv);
                  ExprValue arg3 = arguments.get(2).valueOf(valueEnv);
                  return function.apply(functionProperties, arg1, arg2, arg3);
                }

                @Override
                public ExprType type() {
                  return returnType;
                }

                @Override
                public String toString() {
                  return String.format(
                      "%s(%s)",
                      functionName,
                      arguments.stream().map(Object::toString).collect(Collectors.joining(", ")));
                }
              };
      return Pair.of(functionSignature, functionBuilder);
    };
  }

  /**
   * No Arg Function Implementation.
   *
   * @param function {@link ExprValue} based unary function.
   * @param returnType return type.
   * @return Unary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>> impl(
      SerializableNoArgFunction<ExprValue> function, ExprType returnType) {
    return implWithProperties(fp -> function.get(), returnType);
  }

  /**
   * Unary Function Implementation.
   *
   * @param function {@link ExprValue} based unary function.
   * @param returnType return type.
   * @param argsType argument type.
   * @return Unary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>> impl(
      SerializableFunction<ExprValue, ExprValue> function, ExprType returnType, ExprType argsType) {

    return implWithProperties((fp, arg) -> function.apply(arg), returnType, argsType);
  }

  /**
   * Binary Function Implementation.
   *
   * @param function {@link ExprValue} based unary function.
   * @param returnType return type.
   * @param args1Type argument type.
   * @param args2Type argument type.
   * @return Binary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>> impl(
      SerializableBiFunction<ExprValue, ExprValue, ExprValue> function,
      ExprType returnType,
      ExprType args1Type,
      ExprType args2Type) {

    return implWithProperties(
        (fp, arg1, arg2) -> function.apply(arg1, arg2), returnType, args1Type, args2Type);
  }

  /**
   * Triple Function Implementation.
   *
   * @param function {@link ExprValue} based unary function.
   * @param returnType return type.
   * @param args1Type argument type.
   * @param args2Type argument type.
   * @return Binary Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>> impl(
      SerializableTriFunction<ExprValue, ExprValue, ExprValue, ExprValue> function,
      ExprType returnType,
      ExprType args1Type,
      ExprType args2Type,
      ExprType args3Type) {

    return functionName -> {
      FunctionSignature functionSignature =
          new FunctionSignature(functionName, Arrays.asList(args1Type, args2Type, args3Type));
      FunctionBuilder functionBuilder =
          (functionProperties, arguments) ->
              new FunctionExpression(functionName, arguments) {
                @Override
                public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                  ExprValue arg1 = arguments.get(0).valueOf(valueEnv);
                  ExprValue arg2 = arguments.get(1).valueOf(valueEnv);
                  ExprValue arg3 = arguments.get(2).valueOf(valueEnv);
                  return function.apply(arg1, arg2, arg3);
                }

                @Override
                public ExprType type() {
                  return returnType;
                }

                @Override
                public String toString() {
                  return String.format(
                      "%s(%s, %s, %s)",
                      functionName,
                      arguments.get(0).toString(),
                      arguments.get(1).toString(),
                      arguments.get(2).toString());
                }
              };
      return Pair.of(functionSignature, functionBuilder);
    };
  }

  /**
   * Quadruple Function Implementation.
   *
   * @param function {@link ExprValue} based unary function.
   * @param returnType return type.
   * @param args1Type argument type.
   * @param args2Type argument type.
   * @param args3Type argument type.
   * @return Quadruple Function Implementation.
   */
  public static SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>> impl(
      SerializableQuadFunction<ExprValue, ExprValue, ExprValue, ExprValue, ExprValue> function,
      ExprType returnType,
      ExprType args1Type,
      ExprType args2Type,
      ExprType args3Type,
      ExprType args4Type) {

    return functionName -> {
      FunctionSignature functionSignature =
          new FunctionSignature(
              functionName, Arrays.asList(args1Type, args2Type, args3Type, args4Type));
      FunctionBuilder functionBuilder =
          (functionProperties, arguments) ->
              new FunctionExpression(functionName, arguments) {
                @Override
                public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                  ExprValue arg1 = arguments.get(0).valueOf(valueEnv);
                  ExprValue arg2 = arguments.get(1).valueOf(valueEnv);
                  ExprValue arg3 = arguments.get(2).valueOf(valueEnv);
                  ExprValue arg4 = arguments.get(3).valueOf(valueEnv);
                  return function.apply(arg1, arg2, arg3, arg4);
                }

                @Override
                public ExprType type() {
                  return returnType;
                }

                @Override
                public String toString() {
                  return String.format(
                      "%s(%s, %s, %s, %s)",
                      functionName,
                      arguments.get(0).toString(),
                      arguments.get(1).toString(),
                      arguments.get(2).toString(),
                      arguments.get(3).toString());
                }
              };
      return Pair.of(functionSignature, functionBuilder);
    };
  }

  /** Wrapper the unary ExprValue function with default NULL and MISSING handling. */
  public static SerializableFunction<ExprValue, ExprValue> nullMissingHandling(
      SerializableFunction<ExprValue, ExprValue> function) {
    return value -> {
      if (value.isMissing()) {
        return ExprValueUtils.missingValue();
      } else if (value.isNull()) {
        return ExprValueUtils.nullValue();
      } else {
        return function.apply(value);
      }
    };
  }

  /** Wrapper the binary ExprValue function with default NULL and MISSING handling. */
  public static SerializableBiFunction<ExprValue, ExprValue, ExprValue> nullMissingHandling(
      SerializableBiFunction<ExprValue, ExprValue, ExprValue> function) {
    return (v1, v2) -> {
      if (v1.isMissing() || v2.isMissing()) {
        return ExprValueUtils.missingValue();
      } else if (v1.isNull() || v2.isNull()) {
        return ExprValueUtils.nullValue();
      } else {
        return function.apply(v1, v2);
      }
    };
  }

  /** Wrapper the triple ExprValue function with default NULL and MISSING handling. */
  public SerializableTriFunction<ExprValue, ExprValue, ExprValue, ExprValue> nullMissingHandling(
      SerializableTriFunction<ExprValue, ExprValue, ExprValue, ExprValue> function) {
    return (v1, v2, v3) -> {
      if (v1.isMissing() || v2.isMissing() || v3.isMissing()) {
        return ExprValueUtils.missingValue();
      } else if (v1.isNull() || v2.isNull() || v3.isNull()) {
        return ExprValueUtils.nullValue();
      } else {
        return function.apply(v1, v2, v3);
      }
    };
  }

  /**
   * Wrapper the unary ExprValue function that is aware of FunctionProperties, with default NULL and
   * MISSING handling.
   */
  public static SerializableBiFunction<FunctionProperties, ExprValue, ExprValue>
      nullMissingHandlingWithProperties(
          SerializableBiFunction<FunctionProperties, ExprValue, ExprValue> implementation) {
    return (functionProperties, v1) -> {
      if (v1.isMissing()) {
        return ExprValueUtils.missingValue();
      } else if (v1.isNull()) {
        return ExprValueUtils.nullValue();
      } else {
        return implementation.apply(functionProperties, v1);
      }
    };
  }

  /**
   * Wrapper for the ExprValue function that takes 2 arguments and is aware of FunctionProperties,
   * with default NULL and MISSING handling.
   */
  public static SerializableTriFunction<FunctionProperties, ExprValue, ExprValue, ExprValue>
      nullMissingHandlingWithProperties(
          SerializableTriFunction<FunctionProperties, ExprValue, ExprValue, ExprValue>
              implementation) {
    return (functionProperties, v1, v2) -> {
      if (v1.isMissing() || v2.isMissing()) {
        return ExprValueUtils.missingValue();
      } else if (v1.isNull() || v2.isNull()) {
        return ExprValueUtils.nullValue();
      } else {
        return implementation.apply(functionProperties, v1, v2);
      }
    };
  }

  /**
   * Wrapper for the ExprValue function that takes 3 arguments and is aware of FunctionProperties,
   * with default NULL and MISSING handling.
   */
  public static SerializableQuadFunction<
          FunctionProperties, ExprValue, ExprValue, ExprValue, ExprValue>
      nullMissingHandlingWithProperties(
          SerializableQuadFunction<FunctionProperties, ExprValue, ExprValue, ExprValue, ExprValue>
              implementation) {
    return (functionProperties, v1, v2, v3) -> {
      if (v1.isMissing() || v2.isMissing() || v3.isMissing()) {
        return ExprValueUtils.missingValue();
      }

      if (v1.isNull() || v2.isNull() || v3.isNull()) {
        return ExprValueUtils.nullValue();
      }

      return implementation.apply(functionProperties, v1, v2, v3);
    };
  }
}

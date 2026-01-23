/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
import static org.apache.calcite.sql.type.SqlTypeUtil.createMapType;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.*;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.*;

import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Optionality;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class UserDefinedFunctionUtils {
  public static final RelDataType NULLABLE_DATE_UDT = TYPE_FACTORY.createUDT(EXPR_DATE, true);
  public static final RelDataType NULLABLE_TIME_UDT = TYPE_FACTORY.createUDT(EXPR_TIME, true);
  public static final RelDataType NULLABLE_TIMESTAMP_UDT =
      TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, true);
  public static final RelDataType NULLABLE_STRING =
      TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), true);
  public static final RelDataType NULLABLE_IP_UDT = TYPE_FACTORY.createUDT(EXPR_IP, true);

  public static RelDataType nullablePatternAggList =
      createArrayType(
          TYPE_FACTORY,
          TYPE_FACTORY.createMapType(
              TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
              TYPE_FACTORY.createSqlType(SqlTypeName.ANY)),
          true);
  public static RelDataType patternStruct =
      createMapType(
          TYPE_FACTORY,
          TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
          TYPE_FACTORY.createSqlType(SqlTypeName.ANY),
          false);
  public static RelDataType tokensMap =
      TYPE_FACTORY.createMapType(
          TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
          createArrayType(TYPE_FACTORY, TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), false));
  public static Set<String> SINGLE_FIELD_RELEVANCE_FUNCTION_SET =
      ImmutableSet.of("match", "match_phrase", "match_bool_prefix", "match_phrase_prefix");
  public static Set<String> MULTI_FIELDS_RELEVANCE_FUNCTION_SET =
      ImmutableSet.of("simple_query_string", "query_string", "multi_match");
  public static String IP_FUNCTION_NAME = "IP";

  /**
   * Creates a SqlUserDefinedAggFunction that wraps a Java class implementing an aggregate function.
   *
   * @param udafClass The Java class that implements the UserDefinedAggFunction interface
   * @param functionName The name of the function to be used in SQL statements
   * @param returnType A SqlReturnTypeInference that determines the return type of the function
   * @return A SqlUserDefinedAggFunction that can be used in SQL queries
   */
  public static SqlUserDefinedAggFunction createUserDefinedAggFunction(
      Class<? extends UserDefinedAggFunction<?>> udafClass,
      String functionName,
      SqlReturnTypeInference returnType,
      @Nullable UDFOperandMetadata operandMetadata) {
    return new SqlUserDefinedAggFunction(
        new SqlIdentifier(functionName, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        returnType,
        null,
        operandMetadata,
        Objects.requireNonNull(AggregateFunctionImpl.create(udafClass)),
        false,
        false,
        Optionality.FORBIDDEN);
  }

  /**
   * Creates an aggregate call using the provided SqlAggFunction and arguments.
   *
   * @param aggFunction The aggregate function to call
   * @param fields The primary fields to aggregate
   * @param argList Additional arguments for the aggregate function
   * @param relBuilder The RelBuilder instance used for building relational expressions
   * @return An AggCall object representing the aggregate function call
   */
  public static RelBuilder.AggCall makeAggregateCall(
      SqlAggFunction aggFunction,
      List<RexNode> fields,
      List<RexNode> argList,
      RelBuilder relBuilder) {
    List<RexNode> addArgList = new ArrayList<>(fields);
    addArgList.addAll(argList);
    return relBuilder.aggregateCall(aggFunction, addArgList);
  }

  public static SqlTypeName convertRelDataTypeToSqlTypeName(RelDataType type) {
    if (type instanceof AbstractExprRelDataType<?> exprType) {
      return switch (exprType.getUdt()) {
        case EXPR_DATE -> SqlTypeName.DATE;
        case EXPR_TIME -> SqlTypeName.TIME;
        case EXPR_TIMESTAMP -> SqlTypeName.TIMESTAMP;
        // EXPR_IP is mapped to SqlTypeName.OTHER since there is no
        // corresponding SqlTypeName in Calcite.
        case EXPR_IP -> SqlTypeName.OTHER;
        case EXPR_BINARY -> SqlTypeName.VARBINARY;
        default -> type.getSqlTypeName();
      };
    }
    return type.getSqlTypeName();
  }

  public static FunctionProperties restoreFunctionProperties(DataContext dataContext) {
    long currentTimeInNanos = DataContext.Variable.UTC_TIMESTAMP.get(dataContext);
    Instant instant =
        Instant.ofEpochSecond(
            currentTimeInNanos / 1_000_000_000, currentTimeInNanos % 1_000_000_000);
    ZoneId zoneId = ZoneOffset.UTC;
    return new FunctionProperties(instant, zoneId, QueryType.PPL);
  }

  /**
   * Convert java objects to ExprValue, so that the parameters fit the expr function signature. It
   * invokes ExprValueUtils.fromObjectValue to convert the java objects to ExprValue. Note that
   * date/time/timestamp strings will be converted to strings instead of ExprDateValue, etc.
   *
   * @param operands the operands to convert
   * @param rexCall the RexCall object containing the operands
   * @return the converted operands
   */
  public static List<Expression> convertToExprValues(List<Expression> operands, RexCall rexCall) {
    List<RelDataType> types = rexCall.getOperands().stream().map(RexNode::getType).toList();
    return convertToExprValues(operands, types);
  }

  /**
   * Convert java objects to ExprValue, so that the parameters fit the expr function signature. It
   * invokes ExprValueUtils.fromObjectValue to convert the java objects to ExprValue. Note that
   * date/time/timestamp strings will be converted to strings instead of ExprDateValue, etc.
   *
   * @param operands the operands to convert
   * @return the converted operands
   */
  public static List<Expression> convertToExprValues(
      List<Expression> operands, List<RelDataType> types) {
    List<ExprType> exprTypes =
        types.stream().map(OpenSearchTypeFactory::convertRelDataTypeToExprType).toList();
    List<Expression> exprValues = new ArrayList<>();
    for (int i = 0; i < operands.size(); i++) {
      // TODO a workaround of Apache Calcite bug in 1.41.0:
      // If you call Expressions.convert_(expr, Number.class) or
      // Expressions.convert_(expr, Object.class),
      // you must change to Expressions.convert_(Expressions.box(expr), Number.class/Object.class).
      // Because the codegen in Janino.UnitCompiler, "(Object) -1" will be mistakenly treated to
      // "Object subtracting one" instead of "type casting on native one".
      Expression operand = Expressions.convert_(Expressions.box(operands.get(i)), Object.class);
      exprValues.add(
          i,
          Expressions.call(
              ExprValueUtils.class,
              "fromObjectValue",
              operand,
              Expressions.constant(exprTypes.get(i))));
    }
    return exprValues;
  }

  /**
   * Adapt a static expr method to a UserDefinedFunctionBuilder. It first converts the operands to
   * ExprValue, then calls the method, and finally converts the result to values recognizable by
   * Calcite by calling exprValue.valueForCalcite.
   *
   * @param type the class containing the static method
   * @param methodName the name of the method
   * @param returnTypeInference the return type inference of the UDF
   * @param nullPolicy the null policy of the UDF
   * @param operandMetadata type checker
   * @return an adapted ImplementorUDF with the expr method, which is a UserDefinedFunctionBuilder
   */
  public static ImplementorUDF adaptExprMethodToUDF(
      java.lang.reflect.Type type,
      String methodName,
      SqlReturnTypeInference returnTypeInference,
      NullPolicy nullPolicy,
      @NonNull UDFOperandMetadata operandMetadata) {
    NotNullImplementor implementor =
        (translator, call, translatedOperands) -> {
          List<Expression> operands =
              convertToExprValues(
                  translatedOperands, call.getOperands().stream().map(RexNode::getType).toList());
          Expression exprResult = Expressions.call(type, methodName, operands);
          return Expressions.call(exprResult, "valueForCalcite");
        };
    return new ImplementorUDF(implementor, nullPolicy) {
      @Override
      public SqlReturnTypeInference getReturnTypeInference() {
        return returnTypeInference;
      }

      @Override
      public @NonNull UDFOperandMetadata getOperandMetadata() {
        return operandMetadata;
      }
    };
  }

  /**
   * Adapts a method from the v2 implementation whose parameters include a {@link
   * FunctionProperties} at the beginning to a Calcite-compatible UserDefinedFunctionBuilder.
   */
  public static ImplementorUDF adaptExprMethodWithPropertiesToUDF(
      java.lang.reflect.Type type,
      String methodName,
      SqlReturnTypeInference returnTypeInference,
      NullPolicy nullPolicy,
      UDFOperandMetadata operandMetadata) {
    NotNullImplementor implementor =
        (translator, call, translatedOperands) -> {
          List<Expression> operands =
              convertToExprValues(
                  translatedOperands, call.getOperands().stream().map(RexNode::getType).toList());
          List<Expression> operandsWithProperties = prependFunctionProperties(operands, translator);
          Expression exprResult = Expressions.call(type, methodName, operandsWithProperties);
          return Expressions.call(exprResult, "valueForCalcite");
        };
    return new ImplementorUDF(implementor, nullPolicy) {
      @Override
      public SqlReturnTypeInference getReturnTypeInference() {
        return returnTypeInference;
      }

      @Override
      public @NonNull UDFOperandMetadata getOperandMetadata() {
        return operandMetadata;
      }
    };
  }

  /**
   * Adapt a static math function (e.g., Math.expm1, Math.rint) to a UserDefinedFunctionBuilder.
   * This method generates a Calcite-compatible UDF by boxing the operand, converting it to a
   * double, and then calling the corresponding method in {@link Math}.
   *
   * <p>It assumes the math method has the signature: {@code double method(double)}. This utility is
   * specifically designed for single-operand Math methods.
   *
   * @param methodName the name of the static method in {@link Math} to be invoked
   * @param returnTypeInference the return type inference of the UDF
   * @param nullPolicy the null policy of the UDF
   * @param operandMetadata type checker
   * @return an adapted ImplementorUDF with the math method, which is a UserDefinedFunctionBuilder
   */
  public static ImplementorUDF adaptMathFunctionToUDF(
      String methodName,
      SqlReturnTypeInference returnTypeInference,
      NullPolicy nullPolicy,
      UDFOperandMetadata operandMetadata) {

    NotNullImplementor implementor =
        (translator, call, translatedOperands) -> {
          Expression operand = translatedOperands.get(0);
          operand = Expressions.box(operand);
          operand = Expressions.call(operand, "doubleValue");
          return Expressions.call(Math.class, methodName, operand);
        };

    return new ImplementorUDF(implementor, nullPolicy) {
      @Override
      public SqlReturnTypeInference getReturnTypeInference() {
        return returnTypeInference;
      }

      @Override
      public @NonNull UDFOperandMetadata getOperandMetadata() {
        return operandMetadata;
      }
    };
  }

  public static List<Expression> prependFunctionProperties(
      List<Expression> operands, RexToLixTranslator translator) {
    List<Expression> operandsWithProperties = new ArrayList<>(operands);
    Expression properties =
        Expressions.call(
            UserDefinedFunctionUtils.class, "restoreFunctionProperties", translator.getRoot());
    operandsWithProperties.addFirst(properties);
    return Collections.unmodifiableList(operandsWithProperties);
  }
}

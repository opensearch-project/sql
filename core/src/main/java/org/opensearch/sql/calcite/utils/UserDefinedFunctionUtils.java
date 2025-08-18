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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
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
      SqlReturnTypeInference returnType) {
    return new SqlUserDefinedAggFunction(
        new SqlIdentifier(functionName, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        returnType,
        null,
        null,
        AggregateFunctionImpl.create(udafClass),
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

  /**
   * Creates and registers a User Defined Aggregate Function (UDAF) and returns an AggCall that can
   * be used in query plans.
   *
   * @param udafClass The class implementing the aggregate function behavior
   * @param functionName The name of the aggregate function
   * @param returnType The return type inference for determining the result type
   * @param fields The primary fields to aggregate
   * @param argList Additional arguments for the aggregate function
   * @param relBuilder The RelBuilder instance used for building relational expressions
   * @return An AggCall object representing the aggregate function call
   */
  public static RelBuilder.AggCall createAggregateFunction(
      Class<? extends UserDefinedAggFunction<?>> udafClass,
      String functionName,
      SqlReturnTypeInference returnType,
      List<RexNode> fields,
      List<RexNode> argList,
      RelBuilder relBuilder) {
    SqlUserDefinedAggFunction udaf =
        createUserDefinedAggFunction(udafClass, functionName, returnType);
    return makeAggregateCall(udaf, fields, argList, relBuilder);
  }

  public static SqlReturnTypeInference getReturnTypeInferenceForArray() {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

      // Get argument types
      List<RelDataType> argTypes = opBinding.collectOperandTypes();

      if (argTypes.isEmpty()) {
        throw new IllegalArgumentException("Function requires at least one argument.");
      }
      RelDataType firstArgType = argTypes.getFirst();
      return createArrayType(typeFactory, firstArgType, true);
    };
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

  public static List<Expression> convertToExprValues(List<Expression> operands, RexCall rexCall) {
    List<RelDataType> types = rexCall.getOperands().stream().map(RexNode::getType).toList();
    return convertToExprValues(operands, types);
  }

  public static List<Expression> convertToExprValues(
      List<Expression> operands, List<RelDataType> types) {
    List<ExprType> exprTypes =
        types.stream().map(OpenSearchTypeFactory::convertRelDataTypeToExprType).toList();
    List<Expression> exprValues = new ArrayList<>();
    for (int i = 0; i < operands.size(); i++) {
      Expression operand = Expressions.convert_(operands.get(i), Object.class);
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

  public static ImplementorUDF adaptExprMethodToUDF(
      java.lang.reflect.Type type,
      String methodName,
      SqlReturnTypeInference returnTypeInference,
      NullPolicy nullPolicy,
      @Nullable UDFOperandMetadata operandMetadata) {
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
      public UDFOperandMetadata getOperandMetadata() {
        return operandMetadata;
      }
    };
  }

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
      public UDFOperandMetadata getOperandMetadata() {
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
      public UDFOperandMetadata getOperandMetadata() {
        return operandMetadata;
      }
    };
  }

  public static org.opensearch.sql.data.model.ExprValue enhancedCoalesce(
      org.opensearch.sql.data.model.ExprValue... args) {
    for (org.opensearch.sql.data.model.ExprValue arg : args) {
      if (arg != null && !arg.isNull() && !arg.isMissing()) {
        // Check for empty strings
        if (arg.type().typeName().equals("STRING") && arg.stringValue().trim().isEmpty()) {
          continue;
        }
        return arg;
      }
    }
    return ExprValueUtils.nullValue();
  }
}

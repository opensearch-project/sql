/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.*;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Optionality;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class UserDefinedFunctionUtils {
  public static SqlReturnTypeInference INTEGER_FORCE_NULLABLE =
      ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  public static RelDataType nullableTimeUDT = TYPE_FACTORY.createUDT(EXPR_TIME, true);
  public static RelDataType nullableDateUDT = TYPE_FACTORY.createUDT(EXPR_DATE, true);
  public static RelDataType nullableTimestampUDT =
      TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, true);

  public static SqlReturnTypeInference timestampInference =
      ReturnTypes.explicit(nullableTimestampUDT);

  public static SqlReturnTypeInference timeInference = ReturnTypes.explicit(nullableTimeUDT);

  public static SqlReturnTypeInference dateInference = ReturnTypes.explicit(nullableDateUDT);

  public static RelBuilder.AggCall TransferUserDefinedAggFunction(
      Class<? extends UserDefinedAggFunction> UDAF,
      String functionName,
      SqlReturnTypeInference returnType,
      List<RexNode> fields,
      List<RexNode> argList,
      RelBuilder relBuilder) {
    SqlUserDefinedAggFunction sqlUDAF =
        new SqlUserDefinedAggFunction(
            new SqlIdentifier(functionName, SqlParserPos.ZERO),
            SqlKind.OTHER_FUNCTION,
            returnType,
            null,
            null,
            AggregateFunctionImpl.create(UDAF),
            false,
            false,
            Optionality.FORBIDDEN);
    List<RexNode> addArgList = new ArrayList<>(fields);
    addArgList.addAll(argList);
    return relBuilder.aggregateCall(sqlUDAF, addArgList);
  }

  static SqlReturnTypeInference getReturnTypeInferenceForArray() {
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

  /** Check whether the given array contains null values. */
  public static boolean containsNull(Object[] objects) {
    return Arrays.stream(objects).anyMatch(Objects::isNull);
  }

  public static String formatTimestampWithoutUnnecessaryNanos(LocalDateTime localDateTime) {
    String base = localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    int nano = localDateTime.getNano();
    if (nano == 0) return base;

    String nanoStr = String.format(Locale.ENGLISH, "%09d", nano);
    nanoStr = nanoStr.replaceFirst("0+$", "");
    if (!nanoStr.isEmpty()) {
      return base + "." + nanoStr;
    }
    return base;
  }

  public static SqlTypeName transferDateRelatedTimeName(RexNode candidate) {
    RelDataType type = candidate.getType();
    return OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(type);
  }

  // TODO: pass the function properties directly to the UDF instead of string
  public static FunctionProperties restoreFunctionProperties(DataContext dataContext) {
    long currentTimeInNanos = DataContext.Variable.UTC_TIMESTAMP.get(dataContext);
    Instant instant =
        Instant.ofEpochSecond(
            currentTimeInNanos / 1_000_000_000, currentTimeInNanos % 1_000_000_000);
    TimeZone timeZone = DataContext.Variable.TIME_ZONE.get(dataContext);
    ZoneId zoneId = ZoneId.of(timeZone.getID());
    FunctionProperties functionProperties = new FunctionProperties(instant, zoneId, QueryType.PPL);
    return functionProperties;
  }

  public static List<Expression> addTypeWithCurrentTimestamp(
      List<Expression> candidate, RexCall rexCall, Expression root) {
    List<Expression> newList = new ArrayList<>(candidate);
    for (RexNode rexNode : rexCall.getOperands()) {
      newList.add(Expressions.constant(transferDateRelatedTimeName(rexNode)));
    }
    newList.add(root);
    return newList;
  }

  public static List<Expression> buildArgsWithTypesForExpression(
      List<Expression> candidate, RexCall rexCall, Expression root, int... indexes) {
    List<Expression> result = new ArrayList<>();
    List<RexNode> operands = rexCall.getOperands();
    for (int index : indexes) {
      Expression arg = candidate.get(index);
      result.add(arg);
      result.add(Expressions.constant(transferDateRelatedTimeName(operands.get(index))));
    }
    result.add(root);
    return result;
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
    List<SqlTypeName> sqlTypeNames =
        types.stream().map(OpenSearchTypeFactory::convertRelDataTypeToSqlTypeName).toList();
    List<Expression> exprValues = new ArrayList<>();
    for (int i = 0; i < operands.size(); i++) {
      Expression operand = Expressions.convert_(operands.get(i), Object.class);
      exprValues.add(
          i,
          Expressions.call(
              DateTimeApplyUtils.class,
              "transferInputToExprValue",
              operand,
              Expressions.constant(sqlTypeNames.get(i))));
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
   * @return an adapted ImplementorUDF with the expr method, which is a UserDefinedFunctionBuilder
   */
  public static ImplementorUDF adaptExprMethodToUDF(
      java.lang.reflect.Type type,
      String methodName,
      SqlReturnTypeInference returnTypeInference,
      NullPolicy nullPolicy) {
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
    };
  }

  private static List<Expression> prependTimestampAsProperty(
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
      NullPolicy nullPolicy) {
    NotNullImplementor implementor =
        (translator, call, translatedOperands) -> {
          List<Expression> operands =
              convertToExprValues(
                  translatedOperands, call.getOperands().stream().map(RexNode::getType).toList());
          List<Expression> operandsWithProperties =
              prependTimestampAsProperty(operands, translator);
          Expression exprResult = Expressions.call(type, methodName, operandsWithProperties);
          return Expressions.call(exprResult, "valueForCalcite");
        };
    return new ImplementorUDF(implementor, nullPolicy) {
      @Override
      public SqlReturnTypeInference getReturnTypeInference() {
        return returnTypeInference;
      }
    };
  }
}

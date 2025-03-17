/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
import static org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Optionality;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class UserDefineFunctionUtils {
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

  public static SqlOperator TransferUserDefinedFunction(
      Class<? extends UserDefinedFunction> UDF,
      String functionName,
      SqlReturnTypeInference returnType) {
    final ScalarFunction udfFunction =
        ScalarFunctionImpl.create(Types.lookupMethod(UDF, "eval", Object[].class));
    SqlIdentifier udfLtrimIdentifier =
        new SqlIdentifier(Collections.singletonList(functionName), null, SqlParserPos.ZERO, null);
    return new SqlUserDefinedFunction(
        udfLtrimIdentifier, SqlKind.OTHER_FUNCTION, returnType, null, null, udfFunction);
  }

  static SqlReturnTypeInference getReturnTypeInference(int targetPosition) {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

      // Get argument types
      List<RelDataType> argTypes = opBinding.collectOperandTypes();

      if (argTypes.isEmpty()) {
        throw new IllegalArgumentException("Function requires at least one argument.");
      }
      RelDataType firstArgType = argTypes.get(targetPosition);
      return typeFactory.createSqlType(firstArgType.getSqlTypeName());
    };
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

  static SqlReturnTypeInference getReturnTypeForTimeAddSub() {
    return opBinding -> {
      RelDataType operandType0 = opBinding.getOperandType(0);
      SqlTypeName typeName = operandType0.getSqlTypeName();
      return switch (typeName) {
        case DATE, TIMESTAMP ->
        // Return TIMESTAMP
        opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
        case TIME ->
        // Return TIME
        opBinding.getTypeFactory().createSqlType(SqlTypeName.TIME);
        default -> throw new IllegalArgumentException("Unsupported type: " + typeName);
      };
    };
  }

  /**
   * Create a RelDataType based on a SqlTypeName, then convert it to nullable. This functions
   * effectively the same as <code>ReturnTypes.TYPE_NAME.andThen(SqlTypeTransforms.FORCE_NULLABLE)
   * </code>
   *
   * @param typeName a SqlTypeName
   * @return a nullable RelDataType
   */
  static SqlReturnTypeInference createNullableReturnType(SqlTypeName typeName) {
    return opBinding -> {
      RelDataType relDataType = opBinding.getTypeFactory().createSqlType(typeName);
      return opBinding.getTypeFactory().createTypeWithNullability(relDataType, true);
    };
  }

  static Long transferDateExprToMilliSeconds(String timeExpr) {
    LocalDate date = LocalDate.parse(timeExpr, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
    return date.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
  }

  static List<Integer> transferStringExprToDateValue(String timeExpr) {
    if (timeExpr.contains(":")) {
      // A timestamp
      LocalDateTime localDateTime =
          LocalDateTime.parse(timeExpr, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
      return List.of(
          localDateTime.getYear(), localDateTime.getMonthValue(), localDateTime.getDayOfMonth());
    } else {
      LocalDate localDate = LocalDate.parse(timeExpr, DATE_TIME_FORMATTER_VARIABLE_NANOS_OPTIONAL);
      return List.of(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
    }
  }
}

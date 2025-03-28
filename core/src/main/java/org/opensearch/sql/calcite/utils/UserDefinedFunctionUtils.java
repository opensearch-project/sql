/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

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
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Optionality;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class UserDefinedFunctionUtils {
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
        udfLtrimIdentifier,
        SqlKind.OTHER_FUNCTION,
        returnType,
        InferTypes.ANY_NULLABLE,
        null,
        udfFunction);
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

  /**
   * Infer return argument type as the widest return type among arguments as specified positions.
   * E.g. (Integer, Long) -> Long; (Double, Float, SHORT) -> Double
   *
   * @param positions positions where the return type should be inferred from
   * @param nullable whether the returned value is nullable
   * @return The type inference
   */
  public static SqlReturnTypeInference getLeastRestrictiveReturnTypeAmongArgsAt(
      List<Integer> positions, boolean nullable) {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      List<RelDataType> types = new ArrayList<>();

      for (int position : positions) {
        if (position < 0 || position >= opBinding.getOperandCount()) {
          throw new IllegalArgumentException("Invalid argument position: " + position);
        }
        types.add(opBinding.getOperandType(position));
      }

      RelDataType widerType = typeFactory.leastRestrictive(types);
      if (widerType == null) {
        throw new IllegalArgumentException(
            "Cannot determine a common type for the given positions.");
      }

      return typeFactory.createTypeWithNullability(widerType, nullable);
    };
  }

  /**
   * For some udf/udaf, when giving a list of arguments, we need to infer the return type from the
   * arguments.
   *
   * @param targetPosition
   * @return a inference function
   */
  public static SqlReturnTypeInference getReturnTypeInference(int targetPosition) {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

      // Get argument types
      List<RelDataType> argTypes = opBinding.collectOperandTypes();

      if (argTypes.isEmpty()) {
        throw new IllegalArgumentException("Function requires at least one argument.");
      }
      RelDataType firstArgType = argTypes.get(targetPosition);
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(firstArgType.getSqlTypeName()), true);
    };
  }

  /**
   * For some udf/udaf, We need to create nullable types arguments.
   *
   * @param typeName
   * @return a inference function
   */
  public static SqlReturnTypeInference getNullableReturnTypeInferenceForFixedType(
      SqlTypeName typeName) {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

      // Get argument types
      List<RelDataType> argTypes = opBinding.collectOperandTypes();

      if (argTypes.isEmpty()) {
        throw new IllegalArgumentException("Function requires at least one argument.");
      }
      return typeFactory.createTypeWithNullability(typeFactory.createSqlType(typeName), true);
    };
  }
}

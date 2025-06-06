/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

// TODO: Support array of mixture types.
public class ArrayFunctionImpl extends ImplementorUDF {
  public ArrayFunctionImpl() {
    super(new ArrayImplementor(), NullPolicy.ANY);
  }

  /**
   * @return We wrap it here to accept null since the original return type inference will generate
   *     non-nullable type
   */
  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      try {
        RelDataType originalType =
            SqlLibraryOperators.ARRAY.getReturnTypeInference().inferReturnType(sqlOperatorBinding);
        RelDataType innerType = originalType.getComponentType();
        return createArrayType(
            typeFactory, typeFactory.createTypeWithNullability(innerType, true), true);
      } catch (Exception e) {
        throw new RuntimeException("fail to create array with fixed type: " + e.getMessage());
      }
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class ArrayImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      RelDataType realType = call.getType().getComponentType();
      List<Expression> newArgs = new ArrayList<>();
      newArgs.add(
          Expressions.call(
              Types.lookupMethod(Arrays.class, "asList", Object[].class), translatedOperands));
      assert realType != null;
      newArgs.add(Expressions.constant(realType.getSqlTypeName()));
      return Expressions.call(
          Types.lookupMethod(ArrayFunctionImpl.class, "internalCast", Object[].class), newArgs);
    }
  }

  /**
   * The asList will generate the List<Object>. We need to convert internally, otherwise, the
   * calcite will directly cast like DOUBLE -> INTEGER, which throw error
   */
  public static Object internalCast(Object... args) {
    List<Object> originalList = (List<Object>) args[0];
    SqlTypeName targetType = (SqlTypeName) args[args.length - 1];
    List<Object> result;
    switch (targetType) {
      case DOUBLE:
        result =
            originalList.stream()
                .map(i -> (Object) ((Number) i).doubleValue())
                .collect(Collectors.toList());
        break;
      case FLOAT:
        result =
            originalList.stream()
                .map(i -> (Object) ((Number) i).floatValue())
                .collect(Collectors.toList());
        break;
      case VARCHAR, CHAR:
        result = originalList.stream().map(i -> (Object) i.toString()).collect(Collectors.toList());
        break;
      default:
        result = originalList;
    }
    return result;
  }
}

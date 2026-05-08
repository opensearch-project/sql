/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

import java.math.BigDecimal;
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

/**
 * create an array with input values. We will infer a least restricted type, for example array(1,
 * "demo") -> ["1", "demo"]
 */
public class ArrayFunctionImpl extends ImplementorUDF {
  public ArrayFunctionImpl() {
    super(new ArrayImplementor(), NullPolicy.NONE);
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
        // For empty `array()` Calcite infers element type as NULL, which downstream
        // serializers (notably the analytics-engine route's substrait converter)
        // reject with "Unable to convert the type UNKNOWN". Default to VARCHAR — the
        // result is empty either way, so the chosen scalar element type doesn't
        // affect any value computation, but it gives the call a substrait-serializable
        // type. Existing v2-engine tests (which feed Object lists straight through to
        // ExprCollectionValue) are unaffected because the empty list contains no
        // elements that need to be cast.
        if (innerType == null || isUnknownLikeType(innerType.getSqlTypeName())) {
          innerType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        }
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

  /**
   * Calcite's {@link SqlLibraryOperators#ARRAY} infers a {@code NULL}-element array for an empty
   * call list and an {@code UNKNOWN}-element array when type inference can't pick one (e.g. all
   * operands are typeless nulls). Either of those bubbles up to the analytics-engine route's
   * substrait converter as "Unable to convert the type UNKNOWN" — substrait has no encoding for
   * either marker. Treat both as needing a concrete fallback.
   */
  private static boolean isUnknownLikeType(SqlTypeName sqlTypeName) {
    return sqlTypeName == SqlTypeName.NULL || sqlTypeName == SqlTypeName.UNKNOWN;
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
   * calcite will directly cast like DOUBLE -> INTEGER, which throw error. Null elements are
   * preserved in the array.
   */
  public static Object internalCast(Object... args) {
    List<Object> originalList = (List<Object>) args[0];
    SqlTypeName targetType = (SqlTypeName) args[args.length - 1];
    List<Object> result;
    switch (targetType) {
      case DECIMAL:
        result =
            originalList.stream()
                .map(
                    num -> {
                      if (num == null) {
                        return null;
                      } else if (num instanceof BigDecimal) {
                        return (BigDecimal) num;
                      } else {
                        return BigDecimal.valueOf(((Number) num).doubleValue());
                      }
                    })
                .collect(Collectors.toList());
        break;
      case DOUBLE:
        result =
            originalList.stream()
                .map(i -> i == null ? null : (Object) ((Number) i).doubleValue())
                .collect(Collectors.toList());
        break;
      case FLOAT:
        result =
            originalList.stream()
                .map(i -> i == null ? null : (Object) ((Number) i).floatValue())
                .collect(Collectors.toList());
        break;
      case VARCHAR, CHAR:
        result =
            originalList.stream()
                .map(i -> i == null ? null : (Object) i.toString())
                .collect(Collectors.toList());
        break;
      default:
        result = originalList;
    }
    return result;
  }
}

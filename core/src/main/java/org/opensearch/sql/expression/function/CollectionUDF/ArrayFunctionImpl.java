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
import java.util.stream.IntStream;
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
import org.apache.calcite.util.BuiltInMethod;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class ArrayFunctionImpl extends ImplementorUDF {
  public ArrayFunctionImpl() {
    super(new ArrayImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return SqlLibraryOperators.ARRAY.getReturnTypeInference();
  }

  public static class ArrayImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      RelDataType realType = call.getType().getComponentType();
      List<Expression> newArgs = new ArrayList<>();
      newArgs.add(Expressions.call(Types.lookupMethod(Arrays.class, "asList", Object[].class), translatedOperands));
      assert realType != null;
      newArgs.add(Expressions.constant(realType.getSqlTypeName()));
      return Expressions.call(
          Types.lookupMethod(ArrayFunctionImpl.class, "internalCast", Object[].class), newArgs);
    }
  }

  public static Object internalCast(Object... args) {
    List<Object> originalList = (List<Object>) args[0];
    SqlTypeName targetType = (SqlTypeName) args[args.length - 1];
    List<Object> result;
    switch (targetType) {
      case DOUBLE:
        result = originalList.stream().map(i -> (Object) ( (Number) i).doubleValue()).collect(Collectors.toList());
        break;
      case FLOAT:
        result = originalList.stream().map(i ->  (Object) ( (Number) i).floatValue()).collect(Collectors.toList());
        break;
      default:
        result = originalList;
    }
    return result;
  }
}

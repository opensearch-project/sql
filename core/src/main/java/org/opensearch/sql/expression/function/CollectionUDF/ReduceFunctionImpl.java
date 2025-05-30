/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.opensearch.sql.expression.function.CollectionUDF.LambdaUtils.inferReturnTypeFromLambda;
import static org.opensearch.sql.expression.function.CollectionUDF.LambdaUtils.transferLambdaOutputToTargetType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class ReduceFunctionImpl extends ImplementorUDF {
  public ReduceFunctionImpl() {
    super(new ReduceImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      RexCallBinding rexCallBinding = (RexCallBinding) sqlOperatorBinding;
      List<RexNode> rexNodes = rexCallBinding.operands();
      ArraySqlType listType = (ArraySqlType) rexNodes.get(0).getType();
      RelDataType elementType = listType.getComponentType();
      RelDataType baseType = rexNodes.get(1).getType();
      Map<String, RelDataType> map = new HashMap<>();
      RexLambda mergeLambda = (RexLambda) rexNodes.get(2);
      map.put(mergeLambda.getParameters().get(0).getName(), baseType);
      map.put(mergeLambda.getParameters().get(1).getName(), elementType);
      RelDataType mergedReturnType =
          inferReturnTypeFromLambda((RexLambda) rexNodes.get(2), map, typeFactory);
      if (mergedReturnType != baseType) { // For different acc, we need to recalculate
        map.put(mergeLambda.getParameters().get(0).getName(), mergedReturnType);
        mergedReturnType = inferReturnTypeFromLambda((RexLambda) rexNodes.get(2), map, typeFactory);
      }
      RelDataType finalReturnType;
      if (rexNodes.size() > 3) {
        finalReturnType = inferReturnTypeFromLambda((RexLambda) rexNodes.get(3), map, typeFactory);
      } else {
        finalReturnType = mergedReturnType;
      }
      return finalReturnType;
    };
  }

  public static class ReduceImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      List<Expression> withReturnTypeList = new ArrayList<>(translatedOperands);
      withReturnTypeList.add(Expressions.constant(call.getType().getSqlTypeName()));
      return Expressions.call(
          Types.lookupMethod(ReduceFunctionImpl.class, "eval", Object[].class), withReturnTypeList);
    }
  }

  public static Object eval(Object... args) {
    List<Object> list = (List<Object>) args[0];
    SqlTypeName returnTypes = (SqlTypeName) args[args.length - 1];
    Object base = args[1];
    if (args[2] instanceof org.apache.calcite.linq4j.function.Function2) {
      org.apache.calcite.linq4j.function.Function2 lambdaFunction =
          (org.apache.calcite.linq4j.function.Function2) args[2];

      try {
        for (int i = 0; i < list.size(); i++) {
          base =
              transferLambdaOutputToTargetType(
                  lambdaFunction.apply(base, list.get(i)), returnTypes);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (args.length == 5) {
        if (args[3] instanceof org.apache.calcite.linq4j.function.Function1) {
          return transferLambdaOutputToTargetType(
              ((org.apache.calcite.linq4j.function.Function1) args[3]).apply(base), returnTypes);
        } else {
          throw new IllegalArgumentException("wrong lambda function input");
        }
      } else {
        return base;
      }
    } else {
      throw new IllegalArgumentException("wrong lambda function input");
    }
  }
}

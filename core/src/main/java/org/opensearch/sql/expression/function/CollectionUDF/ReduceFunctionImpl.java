/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.opensearch.sql.expression.function.CollectionUDF.LambdaUtils.inferReturnTypeFromLambda;
import static org.opensearch.sql.expression.function.CollectionUDF.LambdaUtils.transferLambdaOutputToTargetType;

import java.util.ArrayList;
import java.util.List;
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
      List<RexNode> operands = rexCallBinding.operands();
      RelDataType mergedReturnType =
              ((RexLambda) operands.get(2)).getExpression().getType();
      if (operands.size() > 3) {
        RelDataType reduceReturnType =
                ((RexLambda) operands.get(3)).getExpression().getType();
        return typeFactory.leastRestrictive(List.of(mergedReturnType, reduceReturnType));
      }
      return mergedReturnType;
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
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

public class TransformFunctionImpl extends ImplementorUDF {
  public TransformFunctionImpl() {
    super(new TransformImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      RexCallBinding rexCallBinding = (RexCallBinding) sqlOperatorBinding;
      List<RexNode> operands = rexCallBinding.operands();
      RelDataType lambdaReturnType = ((RexLambda) operands.get(1)).getExpression().getType();
      return createArrayType(
          typeFactory, typeFactory.createTypeWithNullability(lambdaReturnType, true), true);
    };
  }

  public static class TransformImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ArraySqlType arrayType = (ArraySqlType) call.getType();
      List<Expression> withReturnTypeList = new ArrayList<>(translatedOperands);
      withReturnTypeList.add(Expressions.constant(arrayType.getComponentType().getSqlTypeName()));
      return Expressions.call(
          Types.lookupMethod(TransformFunctionImpl.class, "eval", Object[].class),
          withReturnTypeList);
    }
  }

  public static Object eval(Object... args) {
    List<Object> target = (List<Object>) args[0];
    List<Object> results = new ArrayList<>();
    SqlTypeName returnType = (SqlTypeName) args[args.length - 1];
    if (args[1] instanceof org.apache.calcite.linq4j.function.Function1) {
      org.apache.calcite.linq4j.function.Function1 lambdaFunction =
          (org.apache.calcite.linq4j.function.Function1) args[1];

      try {
        for (Object candidate : target) {
          results.add(
              transferLambdaOutputToTargetType(lambdaFunction.apply(candidate), returnType));
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return results;
    } else if (args[1] instanceof org.apache.calcite.linq4j.function.Function2) {
      org.apache.calcite.linq4j.function.Function2 lambdaFunction =
          (org.apache.calcite.linq4j.function.Function2) args[1];
      try {
        for (int i = 0; i < target.size(); i++) {
          results.add(
              transferLambdaOutputToTargetType(lambdaFunction.apply(target.get(i), i), returnType));
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return results;
    } else {
      throw new IllegalArgumentException("wrong lambda function input");
    }
  }
}

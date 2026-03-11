/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * The function will first use acc_function to go through all element and return value to the acc.
 * Then apply reduce function to the acc if exists. For example, array=array(1, 2, 3), reduce(array,
 * 0, (acc, x) -> acc + x) = 6, reduce(array, 0, (acc, x) -> acc + x, acc -> acc * 10) = 60
 */
public class ReduceFunctionImpl extends ImplementorUDF {
  public ReduceFunctionImpl() {
    super(new ReduceImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      if (sqlOperatorBinding instanceof RexCallBinding) {
        return sqlOperatorBinding.getOperandType(sqlOperatorBinding.getOperandCount() - 1);
      } else if (sqlOperatorBinding instanceof SqlCallBinding callBinding) {
        RelDataType elementType = callBinding.getOperandType(0).getComponentType();
        RelDataType baseType = callBinding.getOperandType(1);
        SqlLambda reduce1 = callBinding.getCall().operand(2);
        SqlNode function1 = reduce1.getExpression();
        SqlValidator validator = callBinding.getValidator();
        // The saved types are ANY because the lambda function is defined as (ANY, ..) -> ANY
        // Force it to derive types again by removing existing saved types
        validator.removeValidatedNodeType(function1);
        if (function1 instanceof SqlCall call) {
          List<SqlNode> operands = call.getOperandList();
          // The first argument is base (accumulator), while the second is from the array
          if (!operands.isEmpty()) validator.setValidatedNodeType(operands.get(0), baseType);
          if (operands.size() > 1 && elementType != null)
            validator.setValidatedNodeType(operands.get(1), elementType);
        }
        RelDataType returnType = SqlTypeUtil.deriveType(callBinding, function1);
        if (callBinding.getOperandCount() > 3) {
          SqlLambda reduce2 = callBinding.getCall().operand(3);
          SqlNode function2 = reduce2.getExpression();
          validator.removeValidatedNodeType(function2);
          if (function2 instanceof SqlCall call) {
            List<SqlNode> operands = call.getOperandList();
            if (!operands.isEmpty()) validator.setValidatedNodeType(operands.get(0), returnType);
          }
          returnType = SqlTypeUtil.deriveType(callBinding, function2);
        }
        return returnType;
      }
      throw new IllegalStateException(
          StringUtils.format(
              "sqlOperatorBinding can only be either RexCallBinding or SqlCallBinding, but got %s",
              sqlOperatorBinding.getClass()));
    };
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ANY, SqlTypeFamily.FUNCTION)
            .or(
                OperandTypes.family(
                    SqlTypeFamily.ARRAY,
                    SqlTypeFamily.ANY,
                    SqlTypeFamily.FUNCTION,
                    SqlTypeFamily.FUNCTION)));
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

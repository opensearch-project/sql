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
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * The function transform the element of array one by one using lambda. For example, array=array(1,
 * 2, 3), transform(array, x -> x + 2) = [3, 4, 5] Transform can accept one more argument like (x,
 * i) -> x + i, where i is the index of element in array. For example, array=array(1, 2, 3),
 * transform(array, (x, i) -> x + i) = [1, 3, 5]
 */
public class TransformFunctionImpl extends ImplementorUDF {
  public TransformFunctionImpl() {
    super(new TransformImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      RelDataType lambdaReturnType = sqlOperatorBinding.getOperandType(1);
      return createArrayType(
          typeFactory, typeFactory.createTypeWithNullability(lambdaReturnType, true), true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.FUNCTION));
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

    // Check for captured variables: args structure is [array, lambda, captured1, captured2, ...,
    // returnType]
    // If there are more than 3 args (array, lambda, returnType), we have captured variables
    boolean hasCapturedVars = args.length > 3;

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
        if (hasCapturedVars) {
          // Lambda has captured variables - pass the first captured variable as second arg
          // LIMITATION: Currently only the first captured variable (args[2]) is supported.
          // Supporting multiple captured variables would require either:
          // 1. Packing args[2..args.length-1] into an Object[] and modifying lambda generation
          //    to accept a container as the second parameter, or
          // 2. Using higher-arity function interfaces (Function3, Function4, etc.)
          // For now, lambdas that capture multiple external variables may not work correctly.
          Object capturedVar = args[2];
          for (Object candidate : target) {
            results.add(
                transferLambdaOutputToTargetType(
                    lambdaFunction.apply(candidate, capturedVar), returnType));
          }
        } else {
          // Original behavior: lambda with index (x, i) -> expr
          for (int i = 0; i < target.size(); i++) {
            results.add(
                transferLambdaOutputToTargetType(
                    lambdaFunction.apply(target.get(i), i), returnType));
          }
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

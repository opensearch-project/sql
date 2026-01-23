/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
import static org.apache.calcite.util.Static.RESOURCE;
import static org.opensearch.sql.expression.function.CollectionUDF.LambdaUtils.transferLambdaOutputToTargetType;

import java.util.ArrayList;
import java.util.List;
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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.jspecify.annotations.NonNull;
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
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    // Only checks the first two arguments as it allows arbitrary number of arguments to follow them
    return UDFOperandMetadata.wrap(
        new SqlSingleOperandTypeChecker() {
          private static final List<SqlTypeFamily> families =
              List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.FUNCTION);

          /**
           * Copied from {@link FamilyOperandTypeChecker#checkSingleOperandType(SqlCallBinding
           * callBinding, SqlNode node, int iFormalOperand, boolean throwOnFailure)}
           */
          @Override
          public boolean checkSingleOperandType(
              SqlCallBinding callBinding,
              SqlNode operand,
              int iFormalOperand,
              boolean throwOnFailure) {
            // Do not check types after the second operands
            if (iFormalOperand > 1) {
              return true;
            }
            SqlTypeFamily family = families.get(iFormalOperand);
            switch (family) {
              case ANY:
                final RelDataType type = SqlTypeUtil.deriveType(callBinding, operand);
                SqlTypeName typeName = type.getSqlTypeName();

                if (typeName == SqlTypeName.CURSOR) {
                  // We do not allow CURSOR operands, even for ANY
                  if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                  }
                  return false;
                }
              // fall through
              case IGNORE:
                // no need to check
                return true;
              default:
                break;
            }
            if (SqlUtil.isNullLiteral(operand, false)) {
              if (callBinding.isTypeCoercionEnabled()) {
                return true;
              } else if (throwOnFailure) {
                throw callBinding
                    .getValidator()
                    .newValidationError(operand, RESOURCE.nullIllegal());
              } else {
                return false;
              }
            }
            RelDataType type = SqlTypeUtil.deriveType(callBinding, operand);
            SqlTypeName typeName = type.getSqlTypeName();

            // Pass type checking for operators if it's of type 'ANY'.
            if (typeName.getFamily() == SqlTypeFamily.ANY) {
              return true;
            }

            if (!family.getTypeNames().contains(typeName)) {
              if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
              }
              return false;
            }
            return true;
          }

          @Override
          public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!getOperandCountRange().isValidCount(callBinding.getOperandCount())) {
              return false;
            }
            return IntStream.range(0, 2)
                .allMatch(
                    i ->
                        checkSingleOperandType(
                            callBinding, callBinding.operand(i), i, throwOnFailure));
          }

          @Override
          public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.from(2);
          }

          @Override
          public String getAllowedSignatures(SqlOperator op, String opName) {
            return "<ARRAY, FUNCTION, ...ANY OTHER ARGUMENTS>";
          }
        });
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

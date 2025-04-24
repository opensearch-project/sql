/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
import static org.opensearch.sql.expression.function.CollectionUDF.TransformFunctionImpl.decodeBigDecimal;

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
            RelDataType mergedReturnType = inferReturnTypeFromLambda((RexLambda) rexNodes.get(2), List.of(baseType, elementType), typeFactory);
            RelDataType finalReturnType;
            if (rexNodes.size() > 3) {
                finalReturnType = inferReturnTypeFromLambda((RexLambda) rexNodes.get(3), List.of(mergedReturnType), typeFactory);
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
            ScalarFunctionImpl function =
                    (ScalarFunctionImpl)
                            ScalarFunctionImpl.create(
                                    Types.lookupMethod(ReduceFunctionImpl.class, "eval", Object[].class));
            return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
        }
    }

    public static Object eval(Object... args) {
        List<Object> list = (List<Object>) args[0];
        Object base = args[1];
        if (args[2] instanceof org.apache.calcite.linq4j.function.Function2) {
            org.apache.calcite.linq4j.function.Function2 lambdaFunction = (org.apache.calcite.linq4j.function.Function2) args[2];

            try {
                for (int i=0; i<list.size(); i++) {
                    base = decodeBigDecimal(lambdaFunction.apply(base, list.get(i)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (args.length == 4) {
                if (args[3] instanceof org.apache.calcite.linq4j.function.Function1) {
                    return decodeBigDecimal(((org.apache.calcite.linq4j.function.Function1) args[3]).apply(base));
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

    public static RelDataType inferReturnTypeFromLambda(RexLambda rexLambda, List<RelDataType> filledTypes, RelDataTypeFactory typeFactory) {
        RexCall rexCall = (RexCall) rexLambda.getExpression();
        SqlReturnTypeInference returnInfer = rexCall.getOperator().getReturnTypeInference();
        List<RexNode> lambdaOperands = rexCall.getOperands();
        List<RexNode> filledOperands = new ArrayList<>();
        int target_index = 0;
        for (RexNode rexNode : lambdaOperands) {
            if (rexNode instanceof RexLambdaRef rexLambdaRef) {
                filledOperands.add(new RexLambdaRef(rexLambdaRef.getIndex(), rexLambdaRef.getName(), filledTypes.get(target_index)));
                if (target_index + 1 < filledTypes.size()) {
                    target_index++;
                }
            } else {
                filledOperands.add(rexNode);
            }
        }
        return returnInfer.inferReturnType(new RexCallBinding(typeFactory, rexCall.getOperator(), filledOperands, List.of()));
    }
}
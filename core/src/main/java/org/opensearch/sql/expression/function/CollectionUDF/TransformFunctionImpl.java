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
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

public class TransformFunctionImpl extends ImplementorUDF {
    public TransformFunctionImpl() {
        super(new TransformImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return sqlOperatorBinding -> {
            RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
            return createArrayType(
                    typeFactory, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true), true);
        };
    }

    public static class TransformImplementor implements NotNullImplementor {
        @Override
        public Expression implement(
                RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
            ScalarFunctionImpl function =
                    (ScalarFunctionImpl)
                            ScalarFunctionImpl.create(
                                    Types.lookupMethod(TransformFunctionImpl.class, "eval", Object[].class));
            return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
        }
    }

    public static Object eval(Object... args) {
        if (args[1] instanceof org.apache.calcite.linq4j.function.Function1) {
            org.apache.calcite.linq4j.function.Function1 lambdaFunction = (org.apache.calcite.linq4j.function.Function1) args[1];
            List<Object> target = (List<Object>) args[0];
            List<Object> results = new ArrayList<>();
            try {
                for (Object candidate: target) {
                    results.add(decodeBigDecimal(lambdaFunction.apply(candidate)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return results;
        } else if (args[1] instanceof org.apache.calcite.linq4j.function.Function2) {
            org.apache.calcite.linq4j.function.Function2 lambdaFunction = (org.apache.calcite.linq4j.function.Function2) args[1];
            List<Object> target = (List<Object>) args[0];
            List<Object> results = new ArrayList<>();
            try {
                for (int i=0; i<target.size(); i++) {
                    results.add(decodeBigDecimal(lambdaFunction.apply(target.get(i), i)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return results;
        } else {
            throw new IllegalArgumentException("wrong lambda function input");
        }

    }

    public static Object decodeBigDecimal(Object target) {
        if (target instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) target;
            bd = bd.stripTrailingZeros();

            if (target.toString().contains(".")) {
                return bd.doubleValue();
            }

            try {
                return bd.intValueExact();
            } catch (ArithmeticException e) {
                return bd.longValueExact();
            }
        } else {
            return target;
        }
    }


}
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
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;

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
            List<RelDataType> argTypes = sqlOperatorBinding.collectOperandTypes();
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
        org.apache.calcite.linq4j.function.Function1 lambdaFunction = (org.apache.calcite.linq4j.function.Function1) args[1];
        List<Object> target = (List<Object>) args[0];
        List<Object> results = new ArrayList<>();
        try {
            for (Object candidate: target) {
                results.add(lambdaFunction.apply(candidate));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return results;
    }
}
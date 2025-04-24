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
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

public class ReduceFunctionImpl extends ImplementorUDF {
    public ReduceFunctionImpl() {
        super(new ReduceImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return ReturnTypes.ARG1;
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
                    base = lambdaFunction.apply(base, list.get(i));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (args.length == 4) {
                if (args[3] instanceof org.apache.calcite.linq4j.function.Function1) {
                    return ((org.apache.calcite.linq4j.function.Function1) args[3]).apply(base);
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
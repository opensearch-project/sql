/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;

public class ArrayFunctionImpl extends ImplementorUDF {
    public ArrayFunctionImpl() {
        super(new ArrayImplementor(), NullPolicy.ANY);
    }


    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return sqlOperatorBinding -> {
            RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
            List<RelDataType> argTypes = sqlOperatorBinding.collectOperandTypes();
            RelDataType commonType = typeFactory.leastRestrictive(argTypes);
            if (commonType == null) {
                throw new IllegalArgumentException(
                        "All arguments in json array cannot be converted into one common types");
            }
            return createArrayType(
                    typeFactory, typeFactory.createTypeWithNullability(commonType, true), true);
        };
    }


    public static class ArrayImplementor implements NotNullImplementor {
        @Override
        public Expression implement(
                RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
            RelDataType realType = call.getType().getComponentType();
            List<Expression> newArgs = new ArrayList<>(translatedOperands);
            assert realType != null;
            newArgs.add(Expressions.constant(realType.getSqlTypeName()));
            return Expressions.call(
                    Types.lookupMethod(ArrayFunctionImpl.class, "eval", Object[].class), newArgs);
        }
    }


    public static Object eval(Object... args) {
        SqlTypeName targetType = (SqlTypeName) args[args.length - 1];
        switch (targetType) {
            case DOUBLE:
                List<Object> unboxed =
                        IntStream.range(0, args.length - 1)
                                .mapToObj(i -> ((Number) args[i]).doubleValue())
                                .collect(Collectors.toList());


                return unboxed;
            case FLOAT:
                List<Object> unboxedFloat =
                        IntStream.range(0, args.length - 1)
                                .mapToObj(i -> ((Number) args[i]).floatValue())
                                .collect(Collectors.toList());
                return unboxedFloat;
            default:
                return Arrays.asList(args).subList(0, args.length - 1);
        }
    }
}
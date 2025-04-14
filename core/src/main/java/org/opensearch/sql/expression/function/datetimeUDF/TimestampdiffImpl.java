/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.datetimeUDF;

import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.buildArgsWithTypesForExpression;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;

public class TimestampdiffImpl extends ImplementorUDF {
    public TimestampdiffImpl() {
        super(new TimestampdiffImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return ReturnTypes.BIGINT;
    }

    public static class TimestampdiffImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = List.of(list.getFirst());
            newList.addAll(buildArgsWithTypesForExpression(list, rexCall, rexToLixTranslator.getRoot(), 1, 2));
            return Expressions.call( Types.lookupMethod(
                    TosecondsImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        String addUnit = (String) args[0];
        SqlTypeName sqlTypeName1 = (SqlTypeName) args[2];
        FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);

        SqlTypeName sqlTypeName2 = (SqlTypeName) args[4];
        if (sqlTypeName1 == SqlTypeName.TIME || sqlTypeName2 == SqlTypeName.TIME) {
            return exprTimestampDiffForTimeType(
                    restored,
                    new ExprStringValue(addUnit),
                    transferInputToExprValue(args[1], SqlTypeName.TIME),
                    transferInputToExprValue(args[3], SqlTypeName.TIME))
                    .longValue();
        }
        ExprValue timestamp1 = transferInputToExprValue(args[1], sqlTypeName1);
        ExprValue timestamp2 = transferInputToExprValue(args[3], sqlTypeName2);
        ExprValue diffResult = exprTimestampDiff(new ExprStringValue(addUnit), timestamp1, timestamp2);
        return diffResult.longValue();
    }
}

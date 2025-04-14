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
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.List;
import java.util.Objects;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.addTypeWithCurrentTimestamp;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprTimestampValue;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSecondsForIntType;

public class TosecondsImpl extends ImplementorUDF {
    public TosecondsImpl() {
        super(new TosecondsImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return ReturnTypes.BIGINT;
    }

    public static class TosecondsImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = addTypeWithCurrentTimestamp(list, rexCall, rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    TosecondsImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
        SqlTypeName sqlTypeName = (SqlTypeName) args[1];
        switch (sqlTypeName) {
            case DATE, TIME, TIMESTAMP, CHAR, VARCHAR: // need to transfer to timestamp firstly
                ExprValue dateTimeValue = transferInputToExprTimestampValue(args[0], sqlTypeName, restored);
                return exprToSeconds(dateTimeValue).longValue();
            default:
                return exprToSecondsForIntType(new ExprLongValue((Number) args[0])).longValue();
        }
    }
}

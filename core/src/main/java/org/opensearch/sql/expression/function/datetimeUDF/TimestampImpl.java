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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprTimestampValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprAddTime;

public class TimestampImpl extends ImplementorUDF {
    public TimestampImpl() {
        super(new TimestampImplementor(), NullPolicy.ALL);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return timestampInference;
    }

    public static class TimestampImplementor implements NotNullImplementor {

        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = addTypeWithCurrentTimestamp(list, rexCall, rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    TimestampImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(
            Object... args
    ) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        if (args.length == 3) {
            SqlTypeName sqlTypeName = (SqlTypeName) args[1];
            FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
            return transferInputToExprTimestampValue(args[0], sqlTypeName, restored).valueForCalcite();
        } else {
            SqlTypeName sqlTypeName = (SqlTypeName) args[2];
            FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
            ExprValue dateTimeBase = transferInputToExprTimestampValue(args[0], sqlTypeName, restored);
            ExprValue addTime = transferInputToExprTimestampValue(args[1], (SqlTypeName) args[3], restored);
            return exprAddTime(restored, dateTimeBase, addTime).valueForCalcite();
        }
    }



}

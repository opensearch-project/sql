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
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprUtcTime;

public class UTCTimeImpl extends ImplementorUDF {
    public UTCTimeImpl() {
        super(new UTCTimeImplementor(), NullPolicy.ALL);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return timeInference;
    }

    public static class UTCTimeImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = addTypeWithCurrentTimestamp(list, rexCall, rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    UTCTimeImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
        return exprUtcTime(restored).valueForCalcite();
    }
}

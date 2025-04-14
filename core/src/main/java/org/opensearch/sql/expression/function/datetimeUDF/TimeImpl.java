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
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.timeInference;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.transferDateRelatedTimeName;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTime;

public class TimeImpl extends ImplementorUDF {
    public TimeImpl() {
        super(new TimeImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return timeInference;
    }

    public static class TimeImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = new ArrayList<>(list);
            for (RexNode rexNode : rexCall.getOperands()) {
                newList.add(Expressions.constant(transferDateRelatedTimeName(rexNode)));
            }
            return Expressions.call( Types.lookupMethod(
                    TimeImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }

        Object argTime = args[0];
        SqlTypeName argType = (SqlTypeName) args[1];
        return exprTime(transferInputToExprValue(argTime, argType)).valueForCalcite();
    }
}

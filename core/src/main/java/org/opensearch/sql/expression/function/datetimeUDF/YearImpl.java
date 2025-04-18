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
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.List;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferTimeToTimestamp;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYear;

public class YearImpl extends ImplementorUDF {
    public YearImpl() {
        super(new YearImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return INTEGER_FORCE_NULLABLE;
    }

    public static class YearImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = addTypeWithCurrentTimestamp(list, rexCall, rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    YearImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
        ExprValue candidate = transferInputToExprValue(args[0], (SqlTypeName) args[1]);
        if ((SqlTypeName) args[1] == SqlTypeName.TIME) {
            return extractForTime(candidate, restored).valueForCalcite();
        }
        return extract(candidate).valueForCalcite();
    }

    public static ExprValue extractForTime(ExprValue candidate, FunctionProperties functionProperties) {
        return exprYear(transferTimeToTimestamp(candidate, functionProperties));
    }

    public static ExprValue extract(ExprValue candidate) {
        return exprYear(candidate);
    }

}

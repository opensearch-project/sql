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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.List;
import java.util.Objects;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYearweek;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.yearweekToday;

public class YearweekImpl extends ImplementorUDF {
    public YearweekImpl() {
        super(new YearweekImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return INTEGER_FORCE_NULLABLE;
    }

    public static class YearweekImplementor implements NotNullImplementor {

        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = addTypeWithCurrentTimestamp(list, rexCall, rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    YearweekImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {

        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        int mode;
        if (Objects.isNull(args[0])) {
            return null;
        }
        SqlTypeName sqlTypeName;
        ExprValue exprValue;
        if (args.length == 3) {
            sqlTypeName = (SqlTypeName) args[1];
            mode = 0;
        } else {
            sqlTypeName = (SqlTypeName) args[2];
            mode = (int) args[1];
        }
        FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
        if (sqlTypeName == SqlTypeName.TIME) {
            return yearweekToday(new ExprIntegerValue(mode), restored.getQueryStartClock())
                    .integerValue();
        }
        exprValue = transferInputToExprValue(args[0], sqlTypeName);
        ExprValue yearWeekValue = exprYearweek(exprValue, new ExprIntegerValue(mode));
        return yearWeekValue.integerValue();
    }
}

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
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.List;
import java.util.Objects;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.INTEGER_FORCE_NULLABLE;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.addTypeWithCurrentTimestamp;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;

public class WeekImpl extends ImplementorUDF {
    public WeekImpl() {
        super(new WeekImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return INTEGER_FORCE_NULLABLE;
    }

    public static class WeekImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = addTypeWithCurrentTimestamp(list, rexCall, rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    WeekImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {

        if (Objects.isNull(args[0])) {
            return null;
        }

        ExprValue candidate =
                fromObjectValue(args[0], convertSqlTypeNameToExprType((SqlTypeName) args[2]));
        ExprValue woyExpr =
                DateTimeFunctions.exprWeek(candidate, new ExprIntegerValue((Number) args[1]));
        return woyExpr.integerValue();
    }

}

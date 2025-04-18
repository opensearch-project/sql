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
import java.util.Objects;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;

public class UnixTimestampImpl extends ImplementorUDF {
    public UnixTimestampImpl() {
        super(new UnixTimestampImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return ReturnTypes.DOUBLE_FORCE_NULLABLE;
    }

    public static class UnixTimestampImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = addTypeWithCurrentTimestamp(list, rexCall, rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    UnixTimestampImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        if (args.length == 1) {
            FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
            return unixTimeStamp(restored.getQueryStartClock()).longValue();
        }
        Object input = args[0];
        if (Objects.isNull(input)) {
            return null;
        }
        ExprValue candidate =
                fromObjectValue(args[0], convertSqlTypeNameToExprType((SqlTypeName) args[1]));
        return (double) unixTimeStampOf(candidate).longValue();
    }
}

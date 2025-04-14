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
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprDate;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprUtcDate;

public class DateImpl extends ImplementorUDF {
    public DateImpl() {
        super(new DateImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return dateInference;
    }

    public static class DateImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = new ArrayList<>(list);
            for (RexNode rexNode : rexCall.getOperands()) {
                newList.add(Expressions.constant(transferDateRelatedTimeName(rexNode)));
            }
            newList.add(rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    DateImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object eval(Object... args) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
        ExprValue candidate = transferInputToExprValue(args[0], (SqlTypeName) args[1]);
        if ((SqlTypeName) args[1] == SqlTypeName.TIME) {
            return new ExprDateValue(((ExprTimeValue) candidate).dateValue(restored)).valueForCalcite();
        }
        return exprDate(candidate).valueForCalcite();
    }
}

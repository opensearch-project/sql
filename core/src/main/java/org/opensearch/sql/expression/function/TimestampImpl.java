package org.opensearch.sql.expression.function;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.math3.analysis.function.Exp;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimestampFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.QueryType;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprTimestampValue;
import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferInputToExprValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprAddTime;

public class TimestampImpl extends ImplementorUDF {
    protected TimestampImpl() {
        super(new TimestampImplementor(), NullPolicy.ALL);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return timestampInference;
    }

    public static class TimestampImplementor implements NotNullImplementor {

        @Override
        public Expression implement(RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
            List<Expression> newList = new ArrayList<>(list);
            for (RexNode rexNode : rexCall.getOperands()) {
                newList.add(Expressions.constant(transferDateRelatedTimeName(rexNode)));
            }
            newList.add(rexToLixTranslator.getRoot());
            return Expressions.call( Types.lookupMethod(
                    TimestampImpl.class, "eval", Object[].class),  newList);
        }
    }

    public static Object evalOneTimestampValue(
            @Parameter(name = "value") String value,
            @Parameter(name = "type") SqlTypeName type,
            @Parameter(name = "root") DataContext root
    ) {
        long currentTimeInNanos = DataContext.Variable.UTC_TIMESTAMP.get(root);
        Instant instant = Instant.ofEpochSecond(
                currentTimeInNanos / 1_000_000_000,
                currentTimeInNanos % 1_000_000_000
        );
        TimeZone timeZone = DataContext.Variable.TIME_ZONE.get(root);
        ZoneId zoneId = ZoneId.of(timeZone.getID());
        FunctionProperties functionProperties = new FunctionProperties(instant, zoneId, QueryType.PPL);
        return transferInputToExprTimestampValue(value, type, functionProperties).valueForCalcite();
    }

    public static Object eval(
            Object... args
    ) {
        if (UserDefinedFunctionUtils.containsNull(args)) {
            return null;
        }
        if (Objects.isNull(args[0])) {
            return null;
        }
        if (args.length == 3) {
            SqlTypeName sqlTypeName = (SqlTypeName) args[1];
            FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
            return transferInputToExprTimestampValue(args[0], sqlTypeName, restored).valueForCalcite();
        } else {
            SqlTypeName sqlTypeName = (SqlTypeName) args[2];
            ExprValue dateTimeBase = transferInputToExprValue(args[0], sqlTypeName);
            FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
            ExprValue addTime = transferInputToExprValue(args[1], (SqlTypeName) args[3]);
            return exprAddTime(restored, dateTimeBase, addTime).valueForCalcite();
        }
    }



}

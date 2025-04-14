package org.opensearch.sql.calcite.udf.textUDF;

import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.List;

public class LocateFunctionImpl extends ImplementorUDF {
    public LocateFunctionImpl() {
        super(new LocateImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE);
    }

    public static class LocateImplementor implements NotNullImplementor {
        @Override
        public Expression implement(RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
            if (call.getOperands().size() == 3) {
                return Expressions.call(LocateImplementor.class, "locate3", translatedOperands);
            } else {
                return Expressions.call(LocateImplementor.class, "locate2", translatedOperands);
            }
        }

        public static int locate2(String substr, String str) {
            return str.indexOf(substr) + 1;
        }

        public static int locate3(String substr, String str, int start) {
            return str.indexOf(substr, start - 1) + 1;
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.List;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.VARCHAR_FORCE_NULLABLE;
import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.INTEGER_FORCE_NULLABLE;

public class JsonArrayLengthFunctionImpl extends ImplementorUDF {
    public JsonArrayLengthFunctionImpl() {
        super(new JsonArrayLengthImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return INTEGER_FORCE_NULLABLE;
    }

    public static class JsonArrayLengthImplementor implements NotNullImplementor {
        @Override
        public Expression implement(
                RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
            ScalarFunctionImpl function =
                    (ScalarFunctionImpl)
                            ScalarFunctionImpl.create(
                                    Types.lookupMethod(JsonArrayLengthFunctionImpl.class, "eval", Object[].class));
            return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
        }
    }

    public static Object eval(Object... args) {
        assert args.length == 1 : "Json array length only accept one argument";
        String value = (String) args[0];
        try {
            List<?> target = gson.fromJson(value, List.class);
            return target.size();
        } catch (JsonSyntaxException e) {
            return null;
        }
    }
}

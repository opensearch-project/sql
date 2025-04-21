/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;

import java.util.List;
import java.util.Map;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

public class JsonAppendFunctionImpl extends ImplementorUDF {
    public JsonAppendFunctionImpl() {
        super(new JsonAppendImplementor(), NullPolicy.ANY);
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return opBinding -> {
            RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            return typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.ANY)
            );
        };
    }

    public static class JsonAppendImplementor implements NotNullImplementor {
        @Override
        public Expression implement(
                RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
            ScalarFunctionImpl function =
                    (ScalarFunctionImpl)
                            ScalarFunctionImpl.create(
                                    Types.lookupMethod(JsonDeleteFunctionImpl.class, "eval", Object[].class));
            return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
        }
    }

    public static Object eval(Object... args) throws JsonProcessingException {
        String jsonStr = (String) args[0];
        List<String> elements = (List<String>) args[1];
        String demo = updateNestedJson(jsonStr, elements, JsonUtils::appendObjectValue);
        Map<?, ?> result = gson.fromJson(demo, Map.class);
        return result;
    }
}

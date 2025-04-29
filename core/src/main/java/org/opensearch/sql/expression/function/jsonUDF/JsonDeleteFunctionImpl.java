/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
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

public class JsonDeleteFunctionImpl extends ImplementorUDF {
  public JsonDeleteFunctionImpl() {
    super(new JsonDeleteImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      return typeFactory.createMapType(
          typeFactory.createSqlType(SqlTypeName.VARCHAR),
          typeFactory.createSqlType(SqlTypeName.ANY));
    };
  }

  public static class JsonDeleteImplementor implements NotNullImplementor {
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
    Map<String, Object> jsonMap = objectMapper.readValue(jsonStr, Map.class);
    List<String> keys = (List<String>) args[1];
    for (String key : keys) {
      String[] keyParts = key.split("\\.");
      removeNestedKey(jsonMap, keyParts, 0);
    }
    Map<?, ?> result = gson.fromJson(objectMapper.writeValueAsString(jsonMap), Map.class);
    return result;
  }
}

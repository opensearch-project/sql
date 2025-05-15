/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.VARCHAR_FORCE_NULLABLE;
import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;
import static org.opensearch.sql.expression.function.jsonUDF.JsonAppendFunctionImpl.jsonAppendIfArray;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Arrays;
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

public class JsonExtendFunctionImpl extends ImplementorUDF {
  public JsonExtendFunctionImpl() {
    super(new JsonExtendImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return VARCHAR_FORCE_NULLABLE;
  }

  public static class JsonExtendImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonExtendFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) throws JsonProcessingException {
    String jsonStr = (String) args[0];
    List<Object> keys = Arrays.asList(args).subList(1, args.length);
    if (keys.size() % 2 != 0) {
      throw new RuntimeException(
          "Json extend function needs corresponding path and values, but current get: " + keys);
    }
    return jsonAppendIfArray(jsonStr, keys, true);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import java.util.HashMap;
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

public class JsonObjectFunctionImpl extends ImplementorUDF {
  public JsonObjectFunctionImpl() {
    super(new JsonObjectImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      return typeFactory.createMapType(
          typeFactory.createSqlType(SqlTypeName.VARCHAR), // key type
          typeFactory.createSqlType(SqlTypeName.ANY) // value type
          );
    };
  }

  public static class JsonObjectImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonObjectFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    Map<Object, Object> map = new HashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      map.put(args[i], args[i + 1]);
    }
    return map;
  }
}

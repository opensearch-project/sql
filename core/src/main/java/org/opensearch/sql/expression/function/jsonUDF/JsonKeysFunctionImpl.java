/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.apache.calcite.sql.type.SqlTypeUtil.createArrayType;
import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;

import com.google.gson.JsonSyntaxException;
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

public class JsonKeysFunctionImpl extends ImplementorUDF {
  public JsonKeysFunctionImpl() {
    super(new JsonKeysImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      return createArrayType(
          typeFactory,
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR), true),
          true);
    };
  }

  public static class JsonKeysImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonKeysFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    assert args.length == 1 : "Json keys only accept one argument";
    String value = (String) args[0];
    try {
      Map<?, ?> map = gson.fromJson(value, Map.class);
      List<Object> demo = Arrays.asList(map.keySet().toArray());
      return Arrays.asList(map.keySet().toArray());
    } catch (JsonSyntaxException e) {
      return null;
    }
  }
}

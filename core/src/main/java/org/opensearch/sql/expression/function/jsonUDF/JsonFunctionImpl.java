/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.VARCHAR_FORCE_NULLABLE;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * json(value) Evaluates whether a string can be parsed as JSON format. Returns the string value if
 * valid, null otherwise. Argument type: STRING Return type: STRING/NULL
 */
public class JsonFunctionImpl extends ImplementorUDF {
  public JsonFunctionImpl() {
    super(new JsonImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return VARCHAR_FORCE_NULLABLE;
  }

  public static class JsonImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    assert args.length == 1 : "Json only accept one argument";
    String value = (String) args[0];
    try {
      JsonParser.parseString(value);
      return value;
    } catch (JsonSyntaxException e) {
      return null;
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.STRING_FORCE_NULLABLE;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.JsonFunctions;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class JsonAppendFunctionImpl extends ImplementorUDF {
  public JsonAppendFunctionImpl() {
    super(new JsonAppendImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return STRING_FORCE_NULLABLE;
  }

  public static class JsonAppendImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonAppendFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) throws JsonProcessingException {
    String jsonStr = (String) args[0];
    List<Object> keys = Arrays.asList(args).subList(1, args.length);
    if (keys.size() % 2 != 0) {
      throw new RuntimeException(
          "Json append function needs corresponding path and values, but current get: " + keys);
    }
    JsonNode root = convertInputToJsonNode(args[0]);
    List<Object> expands = new ArrayList<>();
    for (int i = 0; i < keys.size(); i += 2) {
      List<String> expandedPaths = expandJsonPath(root, convertToJsonPath(keys.get(i).toString()));
      for (String expandedPath : expandedPaths) {
        expands.add(
            expandedPath
                + MEANING_LESS_KEY_FOR_APPEND_AND_EXTEND); // We add meaningless Key since calcite
        // json_insert can only
        // insert when the path point to null see:
        // https://github.com/apache/calcite/blob/d96709c4cc7ca962601317d0a70914ad95e306e1/core/src/main/java/org/apache/calcite/runtime/JsonFunctions.java#L737
        expands.add(keys.get(i + 1));
      }
    }
    return JsonFunctions.jsonInsert(jsonStr, expands.toArray());
  }
}

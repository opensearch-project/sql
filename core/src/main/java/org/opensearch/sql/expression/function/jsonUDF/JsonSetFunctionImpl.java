/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.STRING_FORCE_NULLABLE;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.Arrays;
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

public class JsonSetFunctionImpl extends ImplementorUDF {
  public JsonSetFunctionImpl() {
    super(new JsonSetImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return STRING_FORCE_NULLABLE;
  }

  public static class JsonSetImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonSetFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    String jsonStr = (String) args[0];
    List<Object> keys = Arrays.asList(args).subList(1, args.length);
    if (keys.size() % 2 != 0) {
      throw new RuntimeException(
          "Json set function needs corresponding path and values, but current get: " + keys);
    }
    return jsonSetIfParentObject(jsonStr, keys);
  }

  public static String jsonSetIfParentObject(Object json, List<Object> fullPathToValue) {
    try {
      JsonNode root = verifyInput(json);

      Configuration conf =
          Configuration.builder()
              .jsonProvider(new JacksonJsonNodeJsonProvider())
              .mappingProvider(new JacksonMappingProvider())
              .build();

      DocumentContext context = JsonPath.using(conf).parse(root);

      for (int index = 0; index < fullPathToValue.size(); index += 2) {
        String fullPath = convertToJsonPath(fullPathToValue.get(index).toString());
        Object value = fullPathToValue.get(index + 1);

        // Split: "$.a.b.d" -> parentPath = "$.a.b", field = "d"
        int lastDot = fullPath.lastIndexOf('.');
        if (lastDot <= 1 || lastDot == fullPath.length() - 1) {
          continue; // Invalid path
        }

        String parentPath = fullPath.substring(0, lastDot);
        String fieldName = fullPath.substring(lastDot + 1);

        JsonNode targets;
        try {
          targets = context.read(parentPath);
        } catch (PathNotFoundException e) {
          continue; // parent path not found
        }

        if (JsonPath.isPathDefinite(parentPath)) {
          if (targets instanceof ObjectNode objectNode) {
            objectNode.set(fieldName, objectMapper.valueToTree(value));
          }
        } else {
          // Some * inside. an arrayNode returned
          for (int i = 0; i < targets.size(); i++) {
            JsonNode target = targets.get(i);
            if (target instanceof ObjectNode objectNode) {
              objectNode.set(fieldName, objectMapper.valueToTree(value));
            }
          }
        }
      }

      return root.toString();
    } catch (Exception e) {
      throw new RuntimeException("jsonSetIfParentObject failed", e);
    }
  }
}

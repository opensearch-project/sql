/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.STRING_FORCE_NULLABLE;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
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
                + ".meaninglessKey"); // We add meaningless Key since calcite json_insert can only
        // insert when the path point to null
        expands.add(keys.get(i + 1));
      }
    }
    return JsonFunctions.jsonInsert(jsonStr, expands.toArray());
  }

  public static String jsonAppendIfArray(Object json, List<Object> pathValueMap, boolean isExtend) {
    try {
      JsonNode tree = convertInputToJsonNode(json);

      Configuration conf =
          Configuration.builder()
              .jsonProvider(new JacksonJsonNodeJsonProvider())
              .mappingProvider(new JacksonMappingProvider())
              .build();

      DocumentContext context = JsonPath.using(conf).parse(tree);

      for (int index = 0; index < pathValueMap.size(); index += 2) {
        String jsonPath = pathValueMap.get(index).toString();
        Object valueToAppend = pathValueMap.get(index + 1);
        JsonNode targets;
        try {
          targets = context.read(jsonPath);
        } catch (PathNotFoundException e) {
          continue;
        }
        if (JsonPath.isPathDefinite(jsonPath)) {
          if (targets instanceof ArrayNode arrayNode) {
            if (isExtend && valueToAppend instanceof List list) {
              for (Object value : list) {
                arrayNode.addPOJO(value);
              }
            } else {
              arrayNode.addPOJO(valueToAppend);
            }
          }
        } else {
          // Some * inside. an arrayNode returned
          for (int i = 0; i < targets.size(); i++) {
            JsonNode target = targets.get(i);
            if (target instanceof ArrayNode arrayNode) {
              if (isExtend && valueToAppend instanceof List list) {
                for (Object value : list) {
                  arrayNode.addPOJO(value);
                }
              } else {
                arrayNode.addPOJO(valueToAppend);
              }
            }
          }
        }
      }
      return tree.toString();
    } catch (Exception e) {
      if (e instanceof PathNotFoundException) {
        return json.toString();
      }
      throw new RuntimeException("Failed to process JSON", e);
    }
  }
}

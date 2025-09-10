/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.MAP_STRING_ANY_FORCE_NULLABLE;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class JsonExtractAllFunctionImpl extends ImplementorUDF {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JsonExtractAllFunctionImpl() {
    super(new JsonExtractAllImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return MAP_STRING_ANY_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class JsonExtractAllImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonExtractAllFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    if (args.length < 1) {
      return null;
    }

    String jsonStr = (String) args[0];
    if (jsonStr == null || jsonStr.trim().isEmpty()) {
      return null;
    }

    try {
      JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonStr);
      if (!jsonNode.isObject()) {
        return null;
      }

      Map<String, Object> resultMap = new HashMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();

      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String key = field.getKey();
        JsonNode value = field.getValue();

        Object extractedValue = extractJsonValue(value);
        resultMap.put(key, extractedValue);
      }

      return resultMap;
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  private static Object extractJsonValue(JsonNode node) {
    if (node.isNull()) {
      return null;
    } else if (node.isBoolean()) {
      return node.booleanValue();
    } else if (node.isInt()) {
      return node.intValue();
    } else if (node.isLong()) {
      return node.longValue();
    } else if (node.isDouble()) {
      return node.doubleValue();
    } else if (node.isTextual()) {
      return node.textValue();
    } else if (node.isArray() || node.isObject()) {
      return node.toString();
    } else {
      return node.asText();
    }
  }
}

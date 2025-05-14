/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;
import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
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
    List<Object> jsonPaths = collectKeyValuePair(args);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(jsonStr);
    DocumentContext ctx = JsonPath.parse(jsonStr);
    for (Object originalPath : jsonPaths) {
      String jsonPath = convertToJsonPath(originalPath.toString());
      try {
        Object matches = ctx.read(jsonPath); // verify whether it's a valid path
      } catch (PathNotFoundException e) {
        return gson.fromJson(jsonStr, Map.class);
      }
      // Resolve path tokens
      PathTokenizer tokenizer = new PathTokenizer(jsonPath);
      root = deletePath(root, tokenizer);
    }
    return gson.fromJson(mapper.writeValueAsString(root), Map.class);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;
import static org.opensearch.sql.expression.DSL.jsonObject;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.utils.JsonUtils;

@UtilityClass
public class JsonFunctions {
  public void register(BuiltinFunctionRepository repository) {
    repository.register(jsonValid());
    repository.register(jsonFunction());
    repository.register(jsonObject());
  }

  private DefaultFunctionResolver jsonValid() {
    return define(
        BuiltinFunctionName.JSON_VALID.getName(),
        impl(nullMissingHandling(JsonUtils::isValidJson), BOOLEAN, STRING));
  }

  private DefaultFunctionResolver jsonFunction() {
    return define(
        BuiltinFunctionName.JSON.getName(),
        impl(nullMissingHandling(JsonUtils::castJson), UNDEFINED, STRING));
  }

  private DefaultFunctionResolver jsonFunction() {
    return define(
        BuiltinFunctionName.JSON.getName(),
        impl(nullMissingHandling(JsonUtils::castJson), UNDEFINED, STRING));
  }

  /** Creates a JSON Object/tuple expr from a given list of kv pairs. */
  private static FunctionResolver jsonObject() {
    return new FunctionResolver() {
      @Override
      public FunctionName getFunctionName() {
        return BuiltinFunctionName.JSON_OBJECT.getName();
      }

      @Override
      public Pair<FunctionSignature, FunctionBuilder> resolve(
          FunctionSignature unresolvedSignature) {
        List<ExprType> paramList = unresolvedSignature.getParamTypeList();
        // check that we got an even number of arguments
        if (paramList.size() % 2 != 0) {
          throw new SemanticCheckException(
              String.format(
                  "Expected an even number of arguments but instead got %d arguments",
                  paramList.size()));
        }

        // check that each "key" argument (of key-value pair) is a string
        for (int i = 0; i < paramList.size(); i = i + 2) {
          ExprType paramType = paramList.get(i);
          if (!ExprCoreType.STRING.equals(paramType)) {
            throw new SemanticCheckException(
                String.format(
                    "Expected type %s instead of %s for parameter #%d",
                    ExprCoreType.STRING, paramType.typeName(), i + 1));
          }
        }

        // return the unresolved signature and function builder
        return Pair.of(
            unresolvedSignature,
            (functionProperties, arguments) ->
                new FunctionExpression(getFunctionName(), arguments) {
                  @Override
                  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                    LinkedHashMap<String, ExprValue> tupleValues = new LinkedHashMap<>();
                    Iterator<Expression> iter = getArguments().iterator();
                    while (iter.hasNext()) {
                      tupleValues.put(
                          iter.next().valueOf(valueEnv).stringValue(),
                          iter.next().valueOf(valueEnv));
                    }
                    return ExprTupleValue.fromExprValueMap(tupleValues);
                  }

                  @Override
                  public ExprType type() {
                    return STRUCT;
                  }
                });
      }
    };
  }
}

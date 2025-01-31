/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.utils.JsonUtils;

@UtilityClass
public class JsonFunctions {
  public void register(BuiltinFunctionRepository repository) {
    repository.register(jsonValid());
    repository.register(jsonFunction());
    repository.register(jsonExtract());
    repository.register(jsonSet());
  }

  private DefaultFunctionResolver jsonValid() {
    return define(
        BuiltinFunctionName.JSON_VALID.getName(), impl(JsonUtils::isValidJson, BOOLEAN, STRING));
  }

  private DefaultFunctionResolver jsonFunction() {
    return define(
        BuiltinFunctionName.JSON.getName(),
        impl(nullMissingHandling(JsonUtils::castJson), UNDEFINED, STRING));
  }

  private DefaultFunctionResolver jsonExtract() {
    return define(
        BuiltinFunctionName.JSON_EXTRACT.getName(),
        impl(JsonUtils::extractJson, UNDEFINED, STRING, STRING),
        impl(JsonUtils::extractJson, UNDEFINED, STRING, STRING, STRING),
        impl(JsonUtils::extractJson, UNDEFINED, STRING, STRING, STRING, STRING));
  }

  private DefaultFunctionResolver jsonSet() {
    return define(
        BuiltinFunctionName.JSON_SET.getName(),
        impl(nullMissingHandling(JsonUtils::setJson), UNDEFINED, STRING, STRING, STRING),
        impl(nullMissingHandling(JsonUtils::setJson), UNDEFINED, STRING, STRING, LONG),
        impl(nullMissingHandling(JsonUtils::setJson), UNDEFINED, STRING, STRING, SHORT),
        impl(nullMissingHandling(JsonUtils::setJson), UNDEFINED, STRING, STRING, INTEGER),
        impl(nullMissingHandling(JsonUtils::setJson), UNDEFINED, STRING, STRING, DOUBLE),
        impl(nullMissingHandling(JsonUtils::setJson), UNDEFINED, STRING, STRING, BOOLEAN),
        impl(nullMissingHandling(JsonUtils::setJson), UNDEFINED, STRING, STRING, ARRAY),
        impl(nullMissingHandling(JsonUtils::setJson), UNDEFINED, STRING, STRING, STRUCT));
  }
}

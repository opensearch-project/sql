/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
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
  }

  private DefaultFunctionResolver jsonValid() {
    return define(
        BuiltinFunctionName.JSON_VALID.getName(), impl(JsonUtils::isValidJson, BOOLEAN, STRING));
  }
}

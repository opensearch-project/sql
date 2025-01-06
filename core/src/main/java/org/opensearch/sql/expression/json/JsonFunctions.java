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

@UtilityClass
public class JsonFunctions {
    public void register(BuiltinFunctionRepository repository) {

        repository.register(jsonValid());
    }

    private DefaultFunctionResolver jsonValid() {
        return define(
                BuiltinFunctionName.JSON_VALID.getName(),
                impl(nullMissingHandling(JsonFunctions::isValidJson), BOOLEAN, STRING));
    }

    /**
     * Checks if given JSON string can be parsed as valid JSON.
     *
     * @param jsonExprValue JSON string (e.g. "198.51.100.14" or "2001:0db8::ff00:42:8329").
     * @return true if the string can be parsed as valid JSON, else false.
     */
    private ExprValue isValidJson(ExprValue jsonExprValue) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.readTree(jsonExprValue.stringValue());
            return ExprValueUtils.LITERAL_TRUE;
        } catch (Exception e) {
            return ExprValueUtils.LITERAL_FALSE;
        }
    }
}

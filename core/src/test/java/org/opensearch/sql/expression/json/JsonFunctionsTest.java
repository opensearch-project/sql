/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
public class JsonFunctionsTest {

    private static final ExprValue JsonObject = ExprValueUtils.stringValue("{\"a\":\"1\",\"b\":\"2\"}");
    private static final ExprValue JsonArray = ExprValueUtils.stringValue("[1, 2, 3, 4]");
    private static final ExprValue JsonScalarString = ExprValueUtils.stringValue("\"abc\"");
    private static final ExprValue JsonEmptyString = ExprValueUtils.stringValue("");
    private static final ExprValue JsonInvalidObject = ExprValueUtils.stringValue("{\"invalid\":\"json\", \"string\"}");
    private static final ExprValue JsonInvalidScalar = ExprValueUtils.stringValue("abc");

    @Mock private Environment<Expression, ExprValue> env;

    @Test
    public void json_valid_invalid_json_string() {
        assertEquals(LITERAL_FALSE, execute(JsonInvalidObject));
        assertEquals(LITERAL_FALSE, execute(JsonInvalidScalar));
    }

    @Test
    public void json_valid_valid_json_string() {
        assertEquals(LITERAL_TRUE, JsonObject);
        assertEquals(LITERAL_TRUE, JsonArray);
        assertEquals(LITERAL_TRUE, JsonScalarString);
        assertEquals(LITERAL_TRUE, JsonEmptyString);
    }

    private ExprValue execute(ExprValue jsonString) {
        final String fieldName = "json_string";
        FunctionExpression exp = DSL.jsonValid(DSL.literal(jsonString));

        when(DSL.ref(fieldName, STRING).valueOf(env)).thenReturn(jsonString);

        return exp.valueOf(env);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.FunctionExpression;

@ExtendWith(MockitoExtension.class)
public class JsonFunctionsTest {
  private static final ExprValue JsonObject =
      ExprValueUtils.stringValue("{\"a\":\"1\",\"b\":\"2\"}");
  private static final ExprValue JsonArray = ExprValueUtils.stringValue("[1, 2, 3, 4]");
  private static final ExprValue JsonScalarString = ExprValueUtils.stringValue("\"abc\"");
  private static final ExprValue JsonEmptyString = ExprValueUtils.stringValue("");
  private static final ExprValue JsonInvalidObject =
      ExprValueUtils.stringValue("{\"invalid\":\"json\", \"string\"}");
  private static final ExprValue JsonInvalidScalar = ExprValueUtils.stringValue("abc");

  @Test
  public void json_valid_returns_false() {
    assertEquals(LITERAL_FALSE, execute(JsonInvalidObject));
    assertEquals(LITERAL_FALSE, execute(JsonInvalidScalar));
  }

  @Test
  public void json_valid_returns_true() {
    assertEquals(LITERAL_TRUE, execute(JsonObject));
    assertEquals(LITERAL_TRUE, execute(JsonArray));
    assertEquals(LITERAL_TRUE, execute(JsonScalarString));
    assertEquals(LITERAL_TRUE, execute(JsonEmptyString));
  }

  private ExprValue execute(ExprValue jsonString) {
    FunctionExpression exp = DSL.jsonValid(DSL.literal(jsonString));
    return exp.valueOf();
  }
}

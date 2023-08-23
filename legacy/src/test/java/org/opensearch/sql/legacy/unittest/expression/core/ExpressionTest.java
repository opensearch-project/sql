/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.expression.core;

import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getNumberValue;

import java.util.Arrays;
import org.json.JSONObject;
import org.opensearch.sql.legacy.expression.core.Expression;
import org.opensearch.sql.legacy.expression.core.ExpressionFactory;
import org.opensearch.sql.legacy.expression.core.operator.ScalarOperation;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;

public class ExpressionTest {
  protected BindingTuple bindingTuple() {
    String json =
        "{\n"
            + "  \"intValue\": 1,\n"
            + "  \"intValue2\": 2,\n"
            + "  \"doubleValue\": 2.0,\n"
            + "  \"negDoubleValue\": -2.0,\n"
            + "  \"stringValue\": \"string\",\n"
            + "  \"booleanValue\": true,\n"
            + "  \"tupleValue\": {\n"
            + "    \"intValue\": 1,\n"
            + "    \"doubleValue\": 2.0,\n"
            + "    \"stringValue\": \"string\"\n"
            + "  },\n"
            + "  \"collectValue\": [\n"
            + "    1,\n"
            + "    2,\n"
            + "    3\n"
            + "  ]\n"
            + "}";
    return BindingTuple.from(new JSONObject(json));
  }

  protected Expression of(ScalarOperation op, Expression... expressions) {
    return ExpressionFactory.of(op, Arrays.asList(expressions));
  }

  protected Number apply(ScalarOperation op, Expression... expressions) {
    return getNumberValue(of(op, expressions).valueOf(bindingTuple()));
  }
}

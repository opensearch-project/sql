/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.nested;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.floatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

public class NestedFunctionTest extends ExpressionTestBase {

  private static final ImmutableMap<String, ExprValue> dataMap = ImmutableMap.of(
      "message.integer_value", integerValue(1),
      "message.long_value", longValue(1L),
      "message.float_value", floatValue(1f),
      "message.double_value", doubleValue(1d),
      "message.boolean_value", booleanValue(true),
      "message.string_value", stringValue("str"),
      "message.struct_value", tupleValue(ImmutableMap.of("str", 1)),
      "message.array_value", collectionValue(ImmutableList.of(1))
  );

  protected static Environment<Expression, ExprValue> env() {
    return var -> {
      return dataMap.get(((ReferenceExpression) var).getAttr());
    };
  }

  static Stream<Map<String, ExprValue>> generateValidData() {
    return Stream.of(
        dataMap
    );
  }

  /**
   * Test single parameter nested function parameters.
   */
  @Test
  public void test_single_param_nested() {
    for (var entry : dataMap.entrySet()) {
      FunctionExpression nested = DSL.nested(DSL.ref(entry.getKey(), entry.getValue().type()));
      assertEquals(entry.getValue(), nested.valueOf(env()));
    }
  }

  /**
   * Test double parameter nested function parameters with second parameter of type STRUCT.
   */
  @Test
  public void test_double_param_nested_struct() {
    for (var entry : dataMap.entrySet()) {
      FunctionExpression nested = DSL.nested(DSL.ref(entry.getKey(), entry.getValue().type()),
          DSL.ref("message", STRUCT));
      assertEquals(entry.getValue(), nested.valueOf(env()));
    }
  }

  /**
   * Test double parameter nested function parameters with second parameter of type ARRAY.
   */
  @Test
  public void test_double_param_nested_array() {
    for (var entry : dataMap.entrySet()) {
      FunctionExpression nested = DSL.nested(DSL.ref(entry.getKey(), entry.getValue().type()),
          DSL.ref("message", ARRAY));
      assertEquals(entry.getValue(), nested.valueOf(env()));
    }
  }
}

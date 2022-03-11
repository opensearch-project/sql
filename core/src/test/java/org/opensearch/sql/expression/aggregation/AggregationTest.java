/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ExpressionTestBase;

public class AggregationTest extends ExpressionTestBase {

  static Map<String, Object> testTupleValue1 = new HashMap<String, Object>() {{
      put("integer_value", 1);
      put("long_value", 1L);
      put("string_value", "f");
      put("double_value", 1d);
      put("float_value", 1f);
      put("date_value", "2020-01-01");
      put("datetime_value", "2020-01-01 00:00:00");
      put("time_value", "00:00:00");
      put("timestamp_value", "2020-01-01 00:00:00");
    }};

  static Map<String, Object> testTupleValue2 = new HashMap<String, Object>() {{
      put("integer_value", 3);
      put("long_value", 3L);
      put("string_value", "m");
      put("double_value", 3d);
      put("float_value", 3f);
      put("date_value", "1970-01-01");
      put("datetime_value", "1970-01-01 19:00:00");
      put("time_value", "19:00:00");
      put("timestamp_value", "1970-01-01 19:00:00");
    }};

  static Map<String, Object> testTupleValue3 = new HashMap<String, Object>() {{
      put("integer_value", 4);
      put("long_value", 4L);
      put("string_value", "n");
      put("double_value", 4d);
      put("float_value", 4f);
      put("date_value", "2040-01-01");
      put("datetime_value", "2040-01-01 07:00:00");
      put("time_value", "07:00:00");
      put("timestamp_value", "2040-01-01 07:00:00");
    }};

  protected static List<ExprValue> tuples =
      Arrays.asList(
          ExprValueUtils.tupleValue(
              new ImmutableMap.Builder<String, Object>()
                  .put("integer_value", 2)
                  .put("long_value", 2L)
                  .put("string_value", "m")
                  .put("double_value", 2d)
                  .put("float_value", 2f)
                  .put("boolean_value", true)
                  .put("struct_value", ImmutableMap.of("str", 1))
                  .put("array_value", ImmutableList.of(1))
                  .put("date_value", "2000-01-01")
                  .put("datetime_value", "2020-01-01 12:00:00")
                  .put("time_value", "12:00:00")
                  .put("timestamp_value", "2020-01-01 12:00:00")
                  .build()),
          ExprValueUtils.tupleValue(testTupleValue1),
          ExprValueUtils.tupleValue(testTupleValue2),
          ExprValueUtils.tupleValue(testTupleValue3));

  protected static List<ExprValue> tuples_with_duplicates =
      Arrays.asList(
          ExprValueUtils.tupleValue(ImmutableMap.of(
              "integer_value", 1,
              "double_value", 4d,
              "struct_value", ImmutableMap.of("str", 1),
              "array_value", ImmutableList.of(1))),
          ExprValueUtils.tupleValue(ImmutableMap.of(
              "integer_value", 1,
              "double_value", 3d,
              "struct_value", ImmutableMap.of("str", 1),
              "array_value", ImmutableList.of(1))),
          ExprValueUtils.tupleValue(ImmutableMap.of(
              "integer_value", 2,
              "double_value", 2d,
              "struct_value", ImmutableMap.of("str", 2),
              "array_value", ImmutableList.of(2))),
          ExprValueUtils.tupleValue(ImmutableMap.of(
              "integer_value", 3,
              "double_value", 1d,
              "struct_value", ImmutableMap.of("str1", 1),
              "array_value", ImmutableList.of(1, 2))));

  protected static List<ExprValue> tuples_with_null_and_missing =
      Arrays.asList(
          ExprValueUtils.tupleValue(
              ImmutableMap.of("integer_value", 2, "string_value", "m", "double_value", 3d)),
          ExprValueUtils.tupleValue(
              ImmutableMap.of("integer_value", 1, "string_value", "f", "double_value", 4d)),
          ExprValueUtils.tupleValue(Collections.singletonMap("double_value", null)));

  protected static List<ExprValue> tuples_with_all_null_or_missing =
      Arrays.asList(
          ExprValueUtils.tupleValue(Collections.singletonMap("integer_value", null)),
          ExprValueUtils.tupleValue(Collections.singletonMap("double", null)),
          ExprValueUtils.tupleValue(Collections.singletonMap("string_value", null)));

  protected ExprValue aggregation(Aggregator aggregator, List<ExprValue> tuples) {
    AggregationState state = aggregator.create();
    for (ExprValue tuple : tuples) {
      aggregator.iterate(tuple.bindingTuples(), state);
    }
    return state.result();
  }
}

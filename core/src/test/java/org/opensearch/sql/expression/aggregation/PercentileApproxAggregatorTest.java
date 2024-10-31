/*
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.type.ExprCoreType.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

@ExtendWith(MockitoExtension.class)
public class PercentileApproxAggregatorTest extends AggregationTest {

  @Mock Expression expression;

  @Mock ExprValue tupleValue;

  @Mock BindingTuple tuple;

  @Test
  public void test_percentile_field_expression() {
    ExprValue result =
        aggregation(DSL.percentile(DSL.ref("integer_value", INTEGER), DSL.literal(50)), tuples);
    assertEquals(3.0, result.value());
    result = aggregation(DSL.percentile(DSL.ref("long_value", LONG), DSL.literal(50)), tuples);
    assertEquals(3.0, result.value());
    result = aggregation(DSL.percentile(DSL.ref("double_value", DOUBLE), DSL.literal(50)), tuples);
    assertEquals(3.0, result.value());
    result = aggregation(DSL.percentile(DSL.ref("float_value", FLOAT), DSL.literal(50)), tuples);
    assertEquals(3.0, result.value());
  }

  @Test
  public void test_percentile_field_expression_with_user_defined_compression() {
    ExprValue result =
        aggregation(
            DSL.percentile(DSL.ref("integer_value", INTEGER), DSL.literal(50), DSL.literal(0.1)),
            tuples);
    assertEquals(2.5, result.value());
    result =
        aggregation(
            DSL.percentile(DSL.ref("long_value", LONG), DSL.literal(50), DSL.literal(0.1)), tuples);
    assertEquals(2.5, result.value());
    result =
        aggregation(
            DSL.percentile(DSL.ref("double_value", DOUBLE), DSL.literal(50), DSL.literal(0.1)),
            tuples);
    assertEquals(2.5, result.value());
    result =
        aggregation(
            DSL.percentile(DSL.ref("float_value", FLOAT), DSL.literal(50), DSL.literal(0.1)),
            tuples);
    assertEquals(2.5, result.value());
  }

  @Test
  public void test_percentile_expression() {
    ExprValue result =
        percentile(
            DSL.literal(50),
            integerValue(0),
            integerValue(1),
            integerValue(2),
            integerValue(3),
            integerValue(4));
    assertEquals(2.0, result.value());
    result = percentile(DSL.literal(30), integerValue(2012), integerValue(2013));
    assertEquals(2012, result.integerValue());
  }

  @Test
  public void test_percentile_with_negative() {
    ExprValue result =
        percentile(
            DSL.literal(50),
            longValue(-100000L),
            longValue(-50000L),
            longValue(40000L),
            longValue(50000L));
    assertEquals(40000.0, result.value());
    ExprValue[] results =
        percentiles(longValue(-100000L), longValue(-50000L), longValue(40000L), longValue(50000L));
    assertPercentileValues(
        results, -100000.0, // p=1.0
        -100000.0, // p=5.0
        -100000.0, // p=10.0
        -100000.0, // p=20.0
        -50000.0, // p=25.0
        -50000.0, // p=30.0
        -50000.0, // p=40.0
        40000.0, // p=50.0
        40000.0, // p=60.0
        40000.0, // p=70.0
        50000.0, // p=75.0
        50000.0, // p=80.0
        50000.0, // p=90.0
        50000.0, // p=95.0
        50000.0, // p=99.0
        50000.0, // p=99.9
        50000.0); // p=100.0
  }

  @Test
  public void test_percentile_value() {
    ExprValue[] results =
        percentiles(
            integerValue(0), integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    assertPercentileValues(
        results, 0.0, // p=1.0
        0.0, // p=5.0
        0.0, // p=10.0
        1.0, // p=20.0
        1.0, // p=25.0
        1.0, // p=30.0
        2.0, // p=40.0
        2.0, // p=50.0
        3.0, // p=60.0
        3.0, // p=70.0
        3.0, // p=75.0
        4.0, // p=80.0
        4.0, // p=90.0
        4.0, // p=95.0
        4.0, // p=99.0
        4.0, // p=99.9
        4.0); // p=100.0
  }

  @Test
  public void test_percentile_with_invalid_size() {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                aggregation(
                    DSL.percentile(DSL.ref("double_value", DOUBLE), DSL.literal(-1)), tuples));
    assertEquals("out of bounds percent value, must be in [0, 100]", exception.getMessage());
    exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                aggregation(
                    DSL.percentile(DSL.ref("double_value", DOUBLE), DSL.literal(200)), tuples));
    assertEquals("out of bounds percent value, must be in [0, 100]", exception.getMessage());
    exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                aggregation(
                    DSL.percentile(
                        DSL.ref("double_value", DOUBLE), DSL.literal(-1), DSL.literal(100)),
                    tuples));
    assertEquals("out of bounds percent value, must be in [0, 100]", exception.getMessage());
    exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                aggregation(
                    DSL.percentile(
                        DSL.ref("double_value", DOUBLE), DSL.literal(200), DSL.literal(100)),
                    tuples));
    assertEquals("out of bounds percent value, must be in [0, 100]", exception.getMessage());
    var exception2 =
        assertThrows(
            ExpressionEvaluationException.class,
            () ->
                aggregation(
                    DSL.percentile(DSL.ref("double_value", DOUBLE), DSL.literal("string")),
                    tuples));
    assertEquals(
        "percentile_approx function expected"
            + " {[INTEGER,DOUBLE],[INTEGER,DOUBLE,DOUBLE],[LONG,DOUBLE],[LONG,DOUBLE,DOUBLE],"
            + "[FLOAT,DOUBLE],[FLOAT,DOUBLE,DOUBLE],[DOUBLE,DOUBLE],[DOUBLE,DOUBLE,DOUBLE]},"
            + " but got [DOUBLE,STRING]",
        exception2.getMessage());
  }

  @Test
  public void test_arithmetic_expression() {
    ExprValue result =
        aggregation(
            DSL.percentile(
                DSL.multiply(
                    DSL.ref("integer_value", INTEGER),
                    DSL.literal(ExprValueUtils.integerValue(10))),
                DSL.literal(50)),
            tuples);
    assertEquals(30.0, result.value());
  }

  @Test
  public void test_filtered_percentile() {
    ExprValue result =
        aggregation(
            DSL.percentile(DSL.ref("integer_value", INTEGER), DSL.literal(50))
                .condition(DSL.greater(DSL.ref("integer_value", INTEGER), DSL.literal(1))),
            tuples);
    assertEquals(3.0, result.value());
  }

  @Test
  public void test_with_missing() {
    ExprValue result =
        aggregation(
            DSL.percentile(DSL.ref("integer_value", INTEGER), DSL.literal(50)),
            tuples_with_null_and_missing);
    assertEquals(2.0, result.value());
  }

  @Test
  public void test_with_null() {
    ExprValue result =
        aggregation(
            DSL.percentile(DSL.ref("double_value", DOUBLE), DSL.literal(50)),
            tuples_with_null_and_missing);
    assertEquals(4.0, result.value());
  }

  @Test
  public void test_with_all_missing_or_null() {
    ExprValue result =
        aggregation(
            DSL.percentile(DSL.ref("integer_value", INTEGER), DSL.literal(50)),
            tuples_with_all_null_or_missing);
    assertTrue(result.isNull());
  }

  @Test
  public void test_unsupported_type() {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new PercentileApproximateAggregator(
                    List.of(DSL.ref("string", STRING), DSL.ref("string", STRING)), STRING));
    assertEquals(
        "percentile aggregation over STRING type is not supported", exception.getMessage());
  }

  @Test
  public void test_to_string() {
    Aggregator aggregator = DSL.percentile(DSL.ref("integer_value", INTEGER), DSL.literal(50));
    assertEquals("percentile(integer_value,50)", aggregator.toString());
    aggregator =
        DSL.percentile(DSL.ref("integer_value", INTEGER), DSL.literal(50), DSL.literal(0.1));
    assertEquals("percentile(integer_value,50,0.1)", aggregator.toString());
  }

  private ExprValue[] percentiles(ExprValue value, ExprValue... values) {
    return new ExprValue[] {
      percentile(DSL.literal(1.0), value, values),
      percentile(DSL.literal(5.0), value, values),
      percentile(DSL.literal(10.0), value, values),
      percentile(DSL.literal(20.0), value, values),
      percentile(DSL.literal(25.0), value, values),
      percentile(DSL.literal(30.0), value, values),
      percentile(DSL.literal(40.0), value, values),
      percentile(DSL.literal(50.0), value, values),
      percentile(DSL.literal(60.0), value, values),
      percentile(DSL.literal(70.0), value, values),
      percentile(DSL.literal(75.0), value, values),
      percentile(DSL.literal(80.0), value, values),
      percentile(DSL.literal(90.0), value, values),
      percentile(DSL.literal(95.0), value, values),
      percentile(DSL.literal(99.0), value, values),
      percentile(DSL.literal(99.9), value, values),
      percentile(DSL.literal(100.0), value, values)
    };
  }

  private void assertPercentileValues(ExprValue[] actualValues, Double... expectedValues) {
    int i = 0;
    for (Double expected : expectedValues) {
      assertEquals(expected, actualValues[i].value());
      i++;
    }
  }

  private ExprValue percentile(LiteralExpression p, ExprValue value, ExprValue... values) {
    when(expression.valueOf(any())).thenReturn(value, values);
    when(expression.type()).thenReturn(DOUBLE);
    return aggregation(DSL.percentile(expression, p), mockTuples(value, values));
  }

  private List<ExprValue> mockTuples(ExprValue value, ExprValue... values) {
    List<ExprValue> mockTuples = new ArrayList<>();
    when(tupleValue.bindingTuples()).thenReturn(tuple);
    mockTuples.add(tupleValue);
    for (ExprValue exprValue : values) {
      mockTuples.add(tupleValue);
    }
    return mockTuples;
  }
}

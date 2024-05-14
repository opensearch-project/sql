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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.*;
import static org.opensearch.sql.data.type.ExprCoreType.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

@ExtendWith(MockitoExtension.class)
public class PercentileAggregatorTest extends AggregationTest {

  @Mock Expression expression;

  @Mock ExprValue tupleValue;

  @Mock BindingTuple tuple;

  @Test
  public void test_percentile_field_expression() {
    ExprValue result =
        aggregation(DSL.percentile(DSL.ref("integer_value", INTEGER), DSL.literal(50)), tuples);
    assertEquals(2.5, result.value());
    result = aggregation(DSL.percentile(DSL.ref("long_value", LONG), DSL.literal(50)), tuples);
    assertEquals(2.5, result.value());
    result = aggregation(DSL.percentile(DSL.ref("double_value", DOUBLE), DSL.literal(50)), tuples);
    assertEquals(2.5, result.value());
    result = aggregation(DSL.percentile(DSL.ref("float_value", FLOAT), DSL.literal(50)), tuples);
    assertEquals(2.5, result.value());
  }

  @Test
  public void test_percentile_disc_field_expression() {
    ExprValue result =
        aggregation(DSL.percentileDisc(DSL.ref("integer_value", INTEGER), DSL.literal(50)), tuples);
    assertEquals(2.0, result.value());
    result = aggregation(DSL.percentileDisc(DSL.ref("long_value", LONG), DSL.literal(50)), tuples);
    assertEquals(2.0, result.value());
    result =
        aggregation(DSL.percentileDisc(DSL.ref("double_value", DOUBLE), DSL.literal(50)), tuples);
    assertEquals(2.0, result.value());
    result =
        aggregation(DSL.percentileDisc(DSL.ref("float_value", FLOAT), DSL.literal(50)), tuples);
    assertEquals(2.0, result.value());
  }

  @Test
  public void test_percentile_expression() {
    //        ExprValue result = percentile(DSL.literal(50),
    //                integerValue(0), integerValue(1), integerValue(2), integerValue(3),
    // integerValue(4));
    //        assertEquals(2.0, result.value());
    ExprValue result = percentile(DSL.literal(30), integerValue(2012), integerValue(2013));
    // This equals to ANSI SQL query
    // SELECT PERCENTILE_DISC(0.3) WITHIN GROUP (ORDER BY col)
    // FROM VALUES (2012), (2013) AS tab(col);
    // PERCENTILE_CONT(0.3) which return 2012.2999999999997 is not yet supported.
    assertEquals(2012, result.integerValue());
  }

  @Test
  public void test_percentile_with_negative() {
    ExprValue result =
        percentile(
            DSL.literal(30),
            longValue(-100000L),
            longValue(-50000L),
            longValue(40000L),
            longValue(50000L));
    // LEGACY -75000.0
    // Discontinuous sample quantile types 1, 2, and 3
    // Continuous sample quantile types 4 through 9
    // Refer https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
    // R_1 -50000.0 (this equal to SparkSQL & POSTGRES PERCENTILE_DISC(0.3))
    // R_2 -50000.0
    // R_3 -100000.0
    // R_4 -90000.0
    // R_5 -65000.0
    // R_6 -75000.0
    // T-digest -55000.0 (this equal to SparkSQL PERCENTILE_CONT(0.3))
    // R_7 -55000.00000000001 (this equal to POSTGRES PERCENTILE_CONT(0.3))
    // R_8 -68333.33333333334
    // R_9 -67500.0
    assertEquals(-55000.00000000001, result.value());
    ExprValue[] results =
        percentiles(longValue(-100000L), longValue(-50000L), longValue(40000L), longValue(50000L));
    for (ExprValue res : results) {
      System.out.println(res.value());
    }
    assertPercentileValues(
        results,
        -85000.0,
        -70000.0,
        -55000.00000000001,
        -31999.999999999985,
        -5000.0,
        21999.999999999985,
        41000.0,
        44000.0,
        47000.0,
        50000.0);
  }

  @Test
  public void test_percentile_disc_with_negative() {
    ExprValue result =
        percentileDisc(
            DSL.literal(30),
            longValue(-100000L),
            longValue(-50000L),
            longValue(40000L),
            longValue(50000L));
    // LEGACY -75000.0
    // Discontinuous sample quantile types 1, 2, and 3
    // Continuous sample quantile types 4 through 9
    // Refer https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
    // R_1 -50000.0 (this equal to SparkSQL & POSTGRES PERCENTILE_DISC(0.3))
    // R_2 -50000.0
    // R_3 -100000.0
    // R_4 -90000.0
    // R_5 -65000.0
    // R_6 -75000.0
    // R_7 -55000.00000000001 (this equal to SparkSQL & POSTGRES PERCENTILE_CONT(0.3))
    // R_8 -68333.33333333334
    // R_9 -67500.0
    assertEquals(-50000.0, result.value());
    ExprValue[] results =
        percentilesDisc(
            longValue(-100000L), longValue(-50000L), longValue(40000L), longValue(50000L));
    assertPercentileValues(
        results, -100000.0, -100000.0, -50000.0, -50000.0, -50000.0, 40000.0, 40000.0, 50000.0,
        50000.0, 50000.0);
  }

  @Test
  public void test_percentile_value() {
    ExprValue[] results =
        percentilesCont(
            integerValue(0), integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    //        for(ExprValue result : results) {
    //            System.out.println(result.value());
    //        }
    assertPercentileValues(
        results,
        0.3999999999999999,
        0.8,
        1.2000000000000002,
        1.6,
        2.0,
        2.4,
        2.8,
        3.2,
        3.5999999999999996,
        4.0);
  }

  @Test
  public void test_percentile_disc_value() {
    ExprValue[] results =
        percentilesDisc(
            integerValue(0), integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    assertPercentileValues(results, 0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0);
  }

  @Test
  public void test_percentile_and_percentile_cont_are_same() {
    ExprValue[] results1 =
        percentiles(
            integerValue(0), integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    ExprValue[] results2 =
        percentilesCont(
            integerValue(0), integerValue(1), integerValue(2), integerValue(3), integerValue(4));
    for (int i = 0; i < results1.length; i++) {
      assertEquals(results1[i].value(), results2[i].value());
    }
  }

  @Test
  public void test_percentile_with_invalid_size() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                aggregation(
                    DSL.percentile(DSL.ref("double_value", DOUBLE), DSL.literal(0)), tuples));
    assertEquals("out of bounds quantile value, must be in (0, 100]", exception.getMessage());
  }

  private ExprValue[] percentiles(ExprValue value, ExprValue... values) {
    return new ExprValue[] {
      percentile(DSL.literal(10), value, values),
      percentile(DSL.literal(20), value, values),
      percentile(DSL.literal(30), value, values),
      percentile(DSL.literal(40), value, values),
      percentile(DSL.literal(50), value, values),
      percentile(DSL.literal(60), value, values),
      percentile(DSL.literal(70), value, values),
      percentile(DSL.literal(80), value, values),
      percentile(DSL.literal(90), value, values),
      percentile(DSL.literal(100), value, values)
    };
  }

  private ExprValue[] percentilesDisc(ExprValue value, ExprValue... values) {
    return new ExprValue[] {
      percentileDisc(DSL.literal(10), value, values),
      percentileDisc(DSL.literal(20), value, values),
      percentileDisc(DSL.literal(30), value, values),
      percentileDisc(DSL.literal(40), value, values),
      percentileDisc(DSL.literal(50), value, values),
      percentileDisc(DSL.literal(60), value, values),
      percentileDisc(DSL.literal(70), value, values),
      percentileDisc(DSL.literal(80), value, values),
      percentileDisc(DSL.literal(90), value, values),
      percentileDisc(DSL.literal(100), value, values)
    };
  }

  private ExprValue[] percentilesCont(ExprValue value, ExprValue... values) {
    return new ExprValue[] {
      percentileCont(DSL.literal(10), value, values),
      percentileCont(DSL.literal(20), value, values),
      percentileCont(DSL.literal(30), value, values),
      percentileCont(DSL.literal(40), value, values),
      percentileCont(DSL.literal(50), value, values),
      percentileCont(DSL.literal(60), value, values),
      percentileCont(DSL.literal(70), value, values),
      percentileCont(DSL.literal(80), value, values),
      percentileCont(DSL.literal(90), value, values),
      percentileCont(DSL.literal(100), value, values)
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

  private ExprValue percentileDisc(LiteralExpression p, ExprValue value, ExprValue... values) {
    when(expression.valueOf(any())).thenReturn(value, values);
    when(expression.type()).thenReturn(DOUBLE);
    return aggregation(DSL.percentileDisc(expression, p), mockTuples(value, values));
  }

  private ExprValue percentileCont(LiteralExpression p, ExprValue value, ExprValue... values) {
    when(expression.valueOf(any())).thenReturn(value, values);
    when(expression.type()).thenReturn(DOUBLE);
    return aggregation(DSL.percentileCont(expression, p), mockTuples(value, values));
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

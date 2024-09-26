/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class FunctionTypeTest {
  @Test
  public void test() {
    assertEquals(FunctionType.AGGREGATE, FunctionType.fromFunctionName("any"));
    assertEquals(FunctionType.AGGREGATE, FunctionType.fromFunctionName("variance"));
    assertEquals(FunctionType.WINDOW, FunctionType.fromFunctionName("cume_dist"));
    assertEquals(FunctionType.WINDOW, FunctionType.fromFunctionName("row_number"));
    assertEquals(FunctionType.ARRAY, FunctionType.fromFunctionName("array"));
    assertEquals(FunctionType.ARRAY, FunctionType.fromFunctionName("sort_array"));
    assertEquals(FunctionType.MAP, FunctionType.fromFunctionName("element_at"));
    assertEquals(FunctionType.MAP, FunctionType.fromFunctionName("try_element_at"));
    assertEquals(FunctionType.DATE_TIMESTAMP, FunctionType.fromFunctionName("add_months"));
    assertEquals(FunctionType.DATE_TIMESTAMP, FunctionType.fromFunctionName("year"));
    assertEquals(FunctionType.JSON, FunctionType.fromFunctionName("from_json"));
    assertEquals(FunctionType.JSON, FunctionType.fromFunctionName("to_json"));
    assertEquals(FunctionType.MATH, FunctionType.fromFunctionName("abs"));
    assertEquals(FunctionType.MATH, FunctionType.fromFunctionName("width_bucket"));
    assertEquals(FunctionType.STRING, FunctionType.fromFunctionName("ascii"));
    assertEquals(FunctionType.STRING, FunctionType.fromFunctionName("upper"));
    assertEquals(FunctionType.CONDITIONAL, FunctionType.fromFunctionName("coalesce"));
    assertEquals(FunctionType.CONDITIONAL, FunctionType.fromFunctionName("nvl2"));
    assertEquals(FunctionType.BITWISE, FunctionType.fromFunctionName("bit_count"));
    assertEquals(FunctionType.BITWISE, FunctionType.fromFunctionName("shiftrightunsigned"));
    assertEquals(FunctionType.CONVERSION, FunctionType.fromFunctionName("bigint"));
    assertEquals(FunctionType.CONVERSION, FunctionType.fromFunctionName("tinyint"));
    assertEquals(FunctionType.PREDICATE, FunctionType.fromFunctionName("isnan"));
    assertEquals(FunctionType.PREDICATE, FunctionType.fromFunctionName("rlike"));
    assertEquals(FunctionType.CSV, FunctionType.fromFunctionName("from_csv"));
    assertEquals(FunctionType.CSV, FunctionType.fromFunctionName("to_csv"));
    assertEquals(FunctionType.MISC, FunctionType.fromFunctionName("aes_decrypt"));
    assertEquals(FunctionType.MISC, FunctionType.fromFunctionName("version"));
    assertEquals(FunctionType.GENERATOR, FunctionType.fromFunctionName("explode"));
    assertEquals(FunctionType.GENERATOR, FunctionType.fromFunctionName("stack"));
    assertEquals(FunctionType.UDF, FunctionType.fromFunctionName("unknown"));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class DataTypeIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(DATA_TYPE_NUMERIC);
    loadIndex(DATA_TYPE_NONNUMERIC);
  }

  @Test
  public void test_numeric_data_types() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s", TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(
        result,
        schema("long_number", "long"),
        schema("integer_number", "integer"),
        schema("short_number", "short"),
        schema("byte_number", "byte"),
        schema("double_number", "double"),
        schema("float_number", "float"),
        schema("half_float_number", "float"),
        schema("scaled_float_number", "double"));
  }

  @Test
  public void test_nonnumeric_data_types() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(
        result,
        schema("boolean_value", "boolean"),
        schema("keyword_value", "string"),
        schema("text_value", "string"),
        schema("binary_value", "binary"),
        schema("date_value", "timestamp"),
        schema("date_nanos_value", "timestamp"),
        schema("ip_value", "ip"),
        schema("object_value", "struct"),
        schema("nested_value", "array"),
        schema("geo_point_value", "geo_point"));
  }

  @Test
  public void test_long_integer_data_type() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " int1 = 2147483647,"
                    + " int2 = -2147483648,"
                    + " long1 = 2147483648,"
                    + " long2 = -2147483649 | "
                    + "fields int1, int2, long1, long2 ",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(
        result,
        schema("int1", "integer"),
        schema("int2", "integer"),
        schema("long1", "long"),
        schema("long2", "long"));
  }

  @Test
  public void test_exponent_literal_converting_to_double_type() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval `9e1` = 9e1, `+9e+1` = +9e+1, `900e-1` = 900e-1, `9.0e1` ="
                    + " 9.0e1, `9.0e+1` = 9.0e+1, `9.0E1` = 9.0E1, `.9e+2` = .9e+2, `0.09e+3` ="
                    + " 0.09e+3, `900.0e-1` = 900.0e-1, `-900.0E-1` = -900.0E-1 | fields `9e1`,"
                    + " `+9e+1`, `900e-1`, `9.0e1`, `9.0e+1`, `9.0E1`, `.9e+2`, `0.09e+3`,"
                    + " `900.0e-1`, `-900.0E-1`",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(
        result,
        schema("9e1", "double"),
        schema("+9e+1", "double"),
        schema("900e-1", "double"),
        schema("9.0e1", "double"),
        schema("9.0e+1", "double"),
        schema("9.0E1", "double"),
        schema(".9e+2", "double"),
        schema("0.09e+3", "double"),
        schema("900.0e-1", "double"),
        schema("-900.0E-1", "double"));
    verifyDataRows(result, rows(90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0, -90.0));
  }
}

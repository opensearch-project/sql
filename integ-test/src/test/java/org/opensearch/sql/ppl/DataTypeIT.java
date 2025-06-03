/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_ALIAS;
import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ALIAS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class DataTypeIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(DATA_TYPE_NUMERIC);
    loadIndex(DATA_TYPE_NONNUMERIC);
    loadIndex(DATA_TYPE_ALIAS);
  }

  @Test
  public void test_numeric_data_types() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s", TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(
        result,
        schema("long_number", "bigint"),
        schema("integer_number", "int"),
        schema("short_number", "smallint"),
        schema("byte_number", "tinyint"),
        schema("double_number", "double"),
        schema("float_number", "float"),
        schema("half_float_number", "float"),
        schema("scaled_float_number", "double"));
  }

  @Test
  public void test_nonnumeric_data_types() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchemaInOrder(
        result,
        schema("text_value", "string"),
        schema("date_nanos_value", "timestamp"),
        schema("date_value", "timestamp"),
        schema("boolean_value", "boolean"),
        schema("ip_value", "ip"),
        schema("nested_value", "array"),
        schema("object_value", "struct"),
        schema("keyword_value", "string"),
        schema("geo_point_value", "geo_point"),
        schema("binary_value", "binary"));
    verifyDataRowsInOrder(
        result,
        rows(
            "text",
            "2019-03-24 01:34:46.123456789",
            "2020-10-13 13:00:00",
            true,
            "127.0.0.1",
            new JSONArray(
                "[{\"last\": \"Smith\", \"first\": \"John\"}, {\"last\": \"White\", \"first\":"
                    + " \"Alice\"}]"),
            new JSONObject("{\"last\": \"Dale\", \"first\": \"Dale\"}"),
            "keyword",
            new JSONObject("{\"lon\": 74, \"lat\": 40.71}"),
            "U29tZSBiaW5hcnkgYmxvYg=="));
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
        schema("int1", "int"),
        schema("int2", "int"),
        schema("long1", "bigint"),
        schema("long2", "bigint"));
  }

  @Test
  public void test_alias_data_type() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where alias_col > 1 | fields original_col, alias_col ",
                TEST_INDEX_ALIAS));
    verifySchema(result, schema("original_col", "int"), schema("alias_col", "int"));
    verifyDataRows(result, rows(2, 2), rows(3, 3));
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

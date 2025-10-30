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
import org.opensearch.client.Request;

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
  public void testNumericFieldFromString() throws Exception {
    final int docId = 2;
    Request insertRequest =
        new Request(
            "PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_DATATYPE_NUMERIC, docId));
    insertRequest.setJsonEntity(
        "{\"long_number\": \"12345678\",\"integer_number\": \"\",\"short_number\":"
            + " \"123\",\"byte_number\": \"12\",\"double_number\": \"1234.5678\",\"float_number\":"
            + " \"\",\"half_float_number\": \"1.23\",\"scaled_float_number\": \"12.34\"}\n");
    client().performRequest(insertRequest);

    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where long_number=12345678 | fields long_number, integer_number,"
                    + " double_number, float_number",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(
        result,
        schema("long_number", "bigint"),
        schema("integer_number", "int"),
        schema("double_number", "double"),
        schema("float_number", "float"));
    verifyDataRows(result, rows(12345678, 0, 1234.5678, 0));

    Request deleteRequest =
        new Request(
            "DELETE",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_DATATYPE_NUMERIC, docId));
    client().performRequest(deleteRequest);
  }

  @Test
  public void testBooleanFieldFromString() throws Exception {
    final int docId = 2;
    Request insertRequest =
        new Request(
            "PUT",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_DATATYPE_NONNUMERIC, docId));
    insertRequest.setJsonEntity("{\"boolean_value\": \"true\", \"keyword_value\": \"test\"}\n");
    client().performRequest(insertRequest);

    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where keyword_value='test' | fields boolean_value",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(result, schema("boolean_value", "boolean"));
    verifyDataRows(result, rows(true));

    Request deleteRequest =
        new Request(
            "DELETE",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_DATATYPE_NONNUMERIC, docId));
    client().performRequest(deleteRequest);
  }
}

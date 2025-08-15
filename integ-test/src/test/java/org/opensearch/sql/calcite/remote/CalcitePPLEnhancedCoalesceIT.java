/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLEnhancedCoalesceIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.STATE_COUNTRY_WITH_NULL);

    Request request1 =
        new Request("PUT", "/" + TEST_INDEX_STATE_COUNTRY_WITH_NULL + "/_doc/9?refresh=true");
    request1.setJsonEntity(
        "{\"name\":null,\"age\":25,\"score\":85.5,\"active\":true,\"year\":2023,\"month\":4}");
    client().performRequest(request1);

    Request request2 =
        new Request("PUT", "/" + TEST_INDEX_STATE_COUNTRY_WITH_NULL + "/_doc/10?refresh=true");
    request2.setJsonEntity(
        "{\"name\":\"\",\"age\":null,\"score\":null,\"active\":false,\"year\":2023,\"month\":4}");
    client().performRequest(request2);
  }

  @Test
  public void testCoalesceBasic() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce(name, age, 0) | fields name, age, result |"
                    + " head 3",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
        actual, schema("name", "string"), schema("age", "int"), schema("result", "string"));
    verifyDataRows(
        actual, rows("Jake", 70, "Jake"), rows("Hello", 30, "Hello"), rows("John", 25, "John"));
  }

  @Test
  public void testCoalesceWithMixedTypes() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce(name, age, 'fallback') |"
                    + " fields name, age, result | head 3",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
        actual, schema("name", "string"), schema("age", "int"), schema("result", "string"));
    verifyDataRows(
        actual, rows("Jake", 70, "Jake"), rows("Hello", 30, "Hello"), rows("John", 25, "John"));
  }

  @Test
  public void testCoalesceWithLiterals() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce(name, 123, 'unknown') | fields name, result |"
                    + " head 1",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("result", "string"));
    verifyDataRows(actual, rows("Jake", "Jake"));
  }

  @Test
  public void testCoalesceInWhere() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where coalesce(name, 'UNKNOWN') = 'Jake' | fields name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));
    verifyDataRows(actual, rows("Jake", 70));
  }

  @Test
  public void testCoalesceWithMultipleFields() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce(name, age, year, month) | fields name, age,"
                    + " year, month, result | head 2",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("year", "int"),
        schema("month", "int"),
        schema("result", "string"));
    verifyDataRows(actual, rows("Jake", 70, 2023, 4, "Jake"), rows("Hello", 30, 2023, 4, "Hello"));
  }

  @Test
  public void testCoalesceNested() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result1 = coalesce(name, 'default'), result2 = coalesce(result1,"
                    + " age) | fields name, age, result1, result2 | head 2",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("result1", "string"),
        schema("result2", "string"));
    verifyDataRows(actual, rows("Jake", 70, "Jake", "Jake"), rows("Hello", 30, "Hello", "Hello"));
  }

  @Test
  public void testCoalesceWithNonExistentField() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce(nonexistent_field, name) | fields name, result"
                    + " | head 2",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("result", "string"));
    verifyDataRows(actual, rows("Jake", "Jake"), rows("Hello", "Hello"));
  }

  @Test
  public void testCoalesceWithMultipleNonExistentFields() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce(field1, field2, name, 'fallback') | fields"
                    + " name, result | head 1",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("result", "string"));
    verifyDataRows(actual, rows("Jake", "Jake"));
  }

  @Test
  public void testCoalesceWithAllNonExistentFields() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce(field1, field2, field3) | fields name, result |"
                    + " head 1",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("result", "string"));
    verifyDataRows(actual, rows("Jake", null));
  }

  @Test
  public void testCoalesceWithEmptyString() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce('', name) | fields name, result | head 1",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("result", "string"));
    verifyDataRows(actual, rows("Jake", ""));
  }

  @Test
  public void testCoalesceWithSpaceString() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = coalesce(' ', name) | fields name, result | head 1",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("result", "string"));
    verifyDataRows(actual, rows("Jake", " "));
  }

  @Test
  public void testCoalesceEmptyFieldWithFallback() throws IOException {

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval empty_field = '' | eval result = coalesce(empty_field, name) |"
                    + " fields name, result | head 1",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("result", "string"));
    verifyDataRows(actual, rows("Jake", ""));
  }
}

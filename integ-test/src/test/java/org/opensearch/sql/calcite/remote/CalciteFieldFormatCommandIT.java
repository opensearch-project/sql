/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteFieldFormatCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);

    // Create test data for string concatenation
    Request request1 = new Request("PUT", "/test_eval/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"Alice\", \"age\": 25, \"title\": \"Engineer\"}");
    client().performRequest(request1);

    Request request2 = new Request("PUT", "/test_eval/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"Bob\", \"age\": 30, \"title\": \"Manager\"}");
    client().performRequest(request2);

    Request request3 = new Request("PUT", "/test_eval/_doc/3?refresh=true");
    request3.setJsonEntity("{\"name\": \"Charlie\", \"age\": null, \"title\": \"Analyst\"}");
    client().performRequest(request3);
  }

  @Test
  public void testFieldFormatStringConcatenation() throws IOException {
    JSONObject result = executeQuery("source=test_eval | fieldformat greeting = 'Hello ' + name");
    verifySchema(
        result,
        schema("name", "string"),
        schema("title", "string"),
        schema("age", "bigint"),
        schema("greeting", "string"));
    verifyDataRows(
        result,
        rows("Alice", "Engineer", 25, "Hello Alice"),
        rows("Bob", "Manager", 30, "Hello Bob"),
        rows("Charlie", "Analyst", null, "Hello Charlie"));
  }

  @Test
  public void testFieldFormatStringConcatenationWithNullFieldToString() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_eval | fieldformat age_desc = \"Age: \".tostring(age,\"commas\") | fields"
                + " name, age, age_desc");
    verifySchema(
        result, schema("name", "string"), schema("age", "bigint"), schema("age_desc", "string"));
    verifyDataRows(
        result,
        rows("Alice", 25, "Age: 25"),
        rows("Bob", 30, "Age: 30"),
        rows("Charlie", null, null));
  }

  @Test
  public void testFieldFormatStringConcatenationWithNullField() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_eval | fieldformat age_desc = \"Age: \".CAST(age AS STRING) | fields name,"
                + " age, age_desc");
    verifySchema(
        result, schema("name", "string"), schema("age", "bigint"), schema("age_desc", "string"));
    verifyDataRows(
        result,
        rows("Alice", 25, "Age: 25"),
        rows("Bob", 30, "Age: 30"),
        rows("Charlie", null, null));
  }

  @Test
  public void testFieldFormatStringConcatWithSuffix() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_eval | fieldformat age_desc = CAST(age AS STRING).\" years\" | fields"
                + " name, age, age_desc");
    verifySchema(
        result, schema("name", "string"), schema("age", "bigint"), schema("age_desc", "string"));
    verifyDataRows(
        result,
        rows("Alice", 25, "25 years"),
        rows("Bob", 30, "30 years"),
        rows("Charlie", null, null));
  }

  @Test
  public void testFieldFormatStringConcatWithPrefixSuffix() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_eval | fieldformat age_desc = \"Age: \".CAST(age AS STRING).\" years\" |"
                + " fields name, age, age_desc");
    verifySchema(
        result, schema("name", "string"), schema("age", "bigint"), schema("age_desc", "string"));
    verifyDataRows(
        result,
        rows("Alice", 25, "Age: 25 years"),
        rows("Bob", 30, "Age: 30 years"),
        rows("Charlie", null, null));
  }
}

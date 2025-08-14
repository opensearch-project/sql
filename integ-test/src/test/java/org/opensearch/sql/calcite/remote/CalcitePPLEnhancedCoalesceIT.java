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
    
    // Add test data with mixed types for enhanced coalesce testing
    Request request1 = new Request("PUT", "/" + TEST_INDEX_STATE_COUNTRY_WITH_NULL + "/_doc/9?refresh=true");
    request1.setJsonEntity("{\"name\":null,\"age\":25,\"score\":85.5,\"active\":true,\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    
    Request request2 = new Request("PUT", "/" + TEST_INDEX_STATE_COUNTRY_WITH_NULL + "/_doc/10?refresh=true");
    request2.setJsonEntity("{\"name\":\"\",\"age\":null,\"score\":null,\"active\":false,\"year\":2023,\"month\":4}");
    client().performRequest(request2);
  }

  @Test
  public void testCoalesceWithMixedTypes() throws IOException {
    // Test enhanced coalesce: should return first non-null value preserving its type
    JSONObject actual = executeQuery(
        String.format(
            "source=%s | where age = 25 | eval result = coalesce(name, age, 'fallback') | fields name, age, result",
            TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, 
        schema("name", "string"), 
        schema("age", "int"), 
        schema("result", "int"));  // Should preserve the type of first non-null (age=25)

    verifyDataRows(actual, rows(null, 25, 25));
  }

  @Test
  public void testCoalesceWithLiteralFallback() throws IOException {
    // Test with literal values as fallback - should preserve type of first non-null
    JSONObject actual = executeQuery(
        String.format(
            "source=%s | eval result = coalesce(name, 123, \"unknown\") | fields name, result | head 3",
            TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, 
        schema("name", "string"), 
        schema("result", "string"));  // First non-null determines type

    // Should return name if not null, otherwise 123, otherwise "unknown"
    verifyDataRows(actual, 
        rows("John", "John"),
        rows("Jane", "Jane"), 
        rows(null, "123"));  // 123 converted to string since name (first arg) is string type
  }

  @Test
  public void testCoalesceWithNumericTypes() throws IOException {
    // Test with different numeric types
    JSONObject actual = executeQuery(
        String.format(
            "source=%s | eval result = coalesce(score, age, 0) | fields score, age, result | head 3",
            TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, 
        schema("score", "double"), 
        schema("age", "int"), 
        schema("result", "double"));
  }

  @Test
  public void testCoalesceWithEmptyStrings() throws IOException {
    // Test enhanced coalesce skips empty strings
    JSONObject actual = executeQuery(
        String.format(
            "source=%s | where name = '' | eval result = coalesce(name, age, 'fallback') | fields name, age, result",
            TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, 
        schema("name", "string"), 
        schema("age", "int"), 
        schema("result", "int"));  // Should skip empty string and use age

    verifyDataRows(actual, rows("", 57, 57));
  }
}
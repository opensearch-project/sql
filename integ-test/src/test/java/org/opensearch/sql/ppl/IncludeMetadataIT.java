/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.TestUtils;

public class IncludeMetadataIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testIncludeMetadataDefaultBehavior() throws IOException {
    // Default behavior should exclude metadata fields
    JSONObject result = executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields * | head 1");

    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"),
        schema("state", "string"));

    assertFalse(
        "Result should not contain _id field",
        result.getJSONArray("schema").toString().contains("_id"));
    assertFalse(
        "Result should not contain _index field",
        result.getJSONArray("schema").toString().contains("_index"));
    assertFalse(
        "Result should not contain _score field",
        result.getJSONArray("schema").toString().contains("_score"));
  }

  @Test
  public void testIncludeMetadataFalseExplicit() throws IOException {
    // Explicitly set include_metadata=false
    JSONObject result =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | fields * | head 1", "include_metadata", "false");

    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"),
        schema("state", "string"));

    assertFalse(
        "Result should not contain _id field",
        result.getJSONArray("schema").toString().contains("_id"));
  }

  @Test
  public void testIncludeMetadataTrue() throws IOException {
    // Set include_metadata=true to include metadata fields
    JSONObject result =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | fields * | head 1", "include_metadata", "true");

    String schemaStr = result.getJSONArray("schema").toString();

    assertTrue("Result should contain account_number field", schemaStr.contains("account_number"));
    assertTrue("Result should contain firstname field", schemaStr.contains("firstname"));

    assertTrue(
        "Result should contain _id field when include_metadata=true", schemaStr.contains("_id"));
    assertTrue(
        "Result should contain _index field when include_metadata=true",
        schemaStr.contains("_index"));
  }

  @Test
  public void testIncludeMetadataWithSpecificFields() throws IOException {
    // When specific fields are selected, include_metadata should not affect the selection
    JSONObject result1 =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname | head 1",
            "include_metadata",
            "false");
    JSONObject result2 =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, lastname | head 1",
            "include_metadata",
            "true");

    verifySchema(result1, schema("firstname", "string"), schema("lastname", "string"));

    verifySchema(result2, schema("firstname", "string"), schema("lastname", "string"));

    assertFalse(
        "Explicit field selection should not include _id even with include_metadata=true",
        result2.getJSONArray("schema").toString().contains("_id"));
  }

  @Test
  public void testIncludeMetadataWithExplicitMetadataField() throws IOException {
    // An explicitly selected metadata field is returned regardless of include_metadata: the
    // parameter only governs selections that return all columns. Both values must behave alike.
    JSONObject withFlagOff =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, _id | head 1",
            "include_metadata",
            "false");
    JSONObject withFlagOn =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, _id | head 1",
            "include_metadata",
            "true");

    verifySchema(withFlagOff, schema("firstname", "string"), schema("_id", "string"));
    verifySchema(withFlagOn, schema("firstname", "string"), schema("_id", "string"));
  }

  @Test
  public void testExplicitMetadataFieldSurvivesLaterCommands() throws IOException {
    // A query that does not end with `fields` gets an implicit all-columns projection attached.
    // An earlier revision of this feature made that projection strip metadata unconditionally,
    // so an explicitly selected _id was silently dropped as soon as another command followed.
    JSONObject sorted =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, _id | sort firstname | head 1",
            "include_metadata",
            "false");
    assertTrue(
        "_id was explicitly selected, so it must survive a following command",
        columnNames(sorted).contains("_id"));

    JSONObject headed =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | fields firstname, _id | head 1",
            "include_metadata",
            "false");
    assertTrue("_id must survive a following head", columnNames(headed).contains("_id"));
  }

  @Test
  public void testIncludeMetadataWithSearch() throws IOException {
    // Test include_metadata with search queries
    JSONObject result =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " \"Amber\" | fields * | head 1",
            "include_metadata",
            "true");

    String schemaStr = result.getJSONArray("schema").toString();

    assertTrue(
        "Search with include_metadata=true should contain regular fields",
        schemaStr.contains("firstname"));
    assertTrue(
        "Search with include_metadata=true should contain _id field", schemaStr.contains("_id"));
    assertTrue(
        "Search with include_metadata=true should contain _score field",
        schemaStr.contains("_score"));
  }

  @Test
  public void testIncludeMetadataWithAggregation() throws IOException {
    // Test that include_metadata doesn't affect aggregation results
    JSONObject result1 =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | stats count() by gender",
            "include_metadata",
            "false");
    JSONObject result2 =
        executeQueryWithParams(
            "source=" + TEST_INDEX_ACCOUNT + " | stats count() by gender",
            "include_metadata",
            "true");

    verifySchema(result1, schema("count()", "bigint"), schema("gender", "string"));

    verifySchema(result2, schema("count()", "bigint"), schema("gender", "string"));

    assertFalse(
        "Aggregation should not include _id field",
        result2.getJSONArray("schema").toString().contains("_id"));
  }

  @Test
  public void testIncludeMetadataWithNestedFields() throws IOException {
    // Test include_metadata behavior with nested/structured data
    loadIndex(Index.NESTED);

    JSONObject result1 =
        executeQueryWithParams(
            "source=" + TEST_INDEX_NESTED_TYPE + " | fields * | head 1",
            "include_metadata",
            "false");
    JSONObject result2 =
        executeQueryWithParams(
            "source=" + TEST_INDEX_NESTED_TYPE + " | fields * | head 1",
            "include_metadata",
            "true");

    String schema1 = result1.getJSONArray("schema").toString();
    String schema2 = result2.getJSONArray("schema").toString();

    assertTrue(
        "Should contain nested fields regardless of include_metadata",
        schema1.contains("message") || schema1.contains("comment") || schema1.contains("myNum"));
    assertTrue(
        "Should contain nested fields regardless of include_metadata",
        schema2.contains("message") || schema2.contains("comment") || schema2.contains("myNum"));

    assertFalse("include_metadata=false should not contain _id", schema1.contains("_id"));
    assertTrue("include_metadata=true should contain _id", schema2.contains("_id"));
  }

  @Test
  public void testIncludeMetadataWithJsonBodyParameter() throws IOException {
    // Test include_metadata parameter in JSON request body
    JSONObject result =
        executeQueryWithJsonBodyParam(
            "source=" + TEST_INDEX_ACCOUNT + " | fields * | head 1", true);

    String schemaStr = result.getJSONArray("schema").toString();

    assertTrue("Result should contain regular fields", schemaStr.contains("firstname"));
    assertTrue(
        "Result should contain _id field when include_metadata=true in JSON body",
        schemaStr.contains("_id"));
    assertTrue(
        "Result should contain _index field when include_metadata=true in JSON body",
        schemaStr.contains("_index"));
  }

  @Test
  public void testRequestBodyTakesPrecedenceOverUrlParameter() throws IOException {
    // Test that request body parameter takes precedence over URL parameter
    Request request = new Request("POST", "/_plugins/_ppl?include_metadata=false");

    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("query", "source=" + TEST_INDEX_ACCOUNT + " | fields * | head 1");
    requestBody.put("include_metadata", true); // Request body says true, URL says false

    String jsonBody = mapper.writeValueAsString(requestBody);
    request.setJsonEntity(jsonBody);

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    JSONObject result = jsonify(TestUtils.getResponseBody(response, true));

    String schemaStr = result.getJSONArray("schema").toString();

    // Should include metadata fields (request body takes precedence)
    assertTrue(
        "Request body should take precedence - should include _id field",
        schemaStr.contains("_id"));
    assertTrue(
        "Request body should take precedence - should include _index field",
        schemaStr.contains("_index"));
  }

  /**
   * Regression test for the V2 (non-Calcite) engine.
   *
   * <p>A query that does not end with an explicit {@code fields} clause gets an implicit
   * all-columns projection attached while the AST is built — a stage shared by both engines. An
   * earlier revision of this feature attached a node type the V2 select-list analyzer has no
   * visitor method for, so the analyzer returned null and every such query failed with HTTP 500
   * NullPointerException.
   *
   * <p>{@code include_metadata} itself is Calcite-only, but the projection it interacts with is
   * shared, so the V2 path must keep working. Calcite is off by default in production, which is why
   * this configuration needs its own coverage.
   */
  @Test
  public void testImplicitSelectAllStillWorksOnV2Engine() throws IOException {
    disableCalcite();
    try {
      // Ends in an aggregation, so an all-columns projection is attached.
      verifyColumn(
          executeQuery("source=" + TEST_INDEX_ACCOUNT + " | stats count()"), columnName("count()"));

      // No fields clause at all.
      JSONObject described = executeQuery("describe " + TEST_INDEX_ACCOUNT);
      assertTrue("describe must return columns", described.getJSONArray("schema").length() > 0);

      // Ends in sort, so an all-columns projection is attached.
      JSONObject sorted = executeQuery("source=" + TEST_INDEX_ACCOUNT + " | sort age | head 1");
      assertTrue("sort must return columns", sorted.getJSONArray("schema").length() > 0);

      // Control: ends with an explicit fields clause, so no projection is attached. This case
      // passed even on the broken revision, which is what made the failure pattern diagnosable.
      verifyColumn(
          executeQuery("source=" + TEST_INDEX_ACCOUNT + " | fields firstname | head 1"),
          columnName("firstname"));
    } finally {
      // enableCalcite/disableCalcite write a persistent cluster setting, so this must be restored
      // or every later test in this class would silently run against the V2 engine.
      enableCalcite();
    }
  }

  @Test
  public void testIncludeMetadataIsIgnoredOnV2Engine() throws IOException {
    // The V2 engine has no metadata-field support, so the parameter is accepted and ignored
    // rather than rejected. Documented here so the behaviour is deliberate, not accidental.
    //
    // Note this uses a query with no `fields` clause rather than `fields *`: the wildcard form is
    // itself Calcite-only (AstBuilder rejects it with "Enhanced fields feature is supported only
    // when plugins.calcite.enabled=true"), so it cannot reach the V2 engine at all.
    disableCalcite();
    try {
      JSONObject result =
          executeQueryWithParams(
              "source=" + TEST_INDEX_ACCOUNT + " | head 1", "include_metadata", "true");
      List<String> columns = columnNames(result);
      assertTrue("V2 must still return data fields", columns.contains("firstname"));
      assertFalse(
          "V2 has no metadata-field support, so include_metadata is ignored",
          columns.contains("_id"));
    } finally {
      enableCalcite();
    }
  }

  /**
   * Extracts column names from a response schema. Preferred over substring matching on the
   * serialised schema, which would match a data field such as {@code user_id} when looking for
   * {@code _id}.
   */
  private List<String> columnNames(JSONObject result) {
    JSONArray schema = result.getJSONArray("schema");
    List<String> names = new ArrayList<>(schema.length());
    for (int i = 0; i < schema.length(); i++) {
      names.add(schema.getJSONObject(i).getString("name"));
    }
    return names;
  }

  private JSONObject executeQueryWithJsonBodyParam(String query, boolean includeMetadata)
      throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");

    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("query", query);
    requestBody.put("include_metadata", includeMetadata);

    String jsonBody = mapper.writeValueAsString(requestBody);
    request.setJsonEntity(jsonBody);

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return jsonify(TestUtils.getResponseBody(response, true));
  }

  private JSONObject executeQueryWithParams(String query, String paramName, String paramValue)
      throws IOException {
    String endpoint = String.format("/_plugins/_ppl?%s=%s", paramName, paramValue);
    Request request = new Request("POST", endpoint);

    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("query", query);

    String jsonBody = mapper.writeValueAsString(requestBody);
    request.setJsonEntity(jsonBody);

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return jsonify(TestUtils.getResponseBody(response, true));
  }
}

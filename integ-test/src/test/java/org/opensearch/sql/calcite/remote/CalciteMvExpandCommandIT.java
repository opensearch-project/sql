/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for mvexpand behavior via Calcite translation.
 *
 * <p>- Uses a canonical shared fixture (created in init()) for the common cases. - Creates small,
 * isolated temp indices per-test for mapping-specific edge cases so tests are deterministic and do
 * not interfere with the shared fixture.
 *
 * <p>NOTE: documents in the canonical fixture are limited to the records exercised by tests to
 * avoid unused-record churn and reviewer comments about unused data.
 */
public class CalciteMvExpandCommandIT extends PPLIntegTestCase {

  private static final String INDEX = Index.MVEXPAND_EDGE_CASES.getName();

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    deleteIndexIfExists(INDEX);

    final String nestedMapping =
        "{ \"mappings\": { \"properties\": { "
            + "\"username\": { \"type\": \"keyword\" },"
            + "\"skills\": { \"type\": \"nested\" }"
            + "} } }";

    createIndex(INDEX, nestedMapping);

    // Canonical fixture documents: only include records actually asserted by tests to avoid
    // reviewer complaints about unused records.
    bulkInsert(
        INDEX,
        "{\"username\":\"happy\",\"skills\":[{\"name\":\"python\"},{\"name\":\"java\"},{\"name\":\"sql\"}]}",
        "{\"username\":\"single\",\"skills\":[{\"name\":\"go\"}]}",
        "{\"username\":\"empty\",\"skills\":[]}",
        "{\"username\":\"nullskills\",\"skills\":null}",
        "{\"username\":\"noskills\"}",
        "{\"username\":\"partial\",\"skills\":[{\"name\":\"kotlin\"},{\"level\":\"intern\"},{\"name\":null}]}",
        "{\"username\":\"mixed_shapes\",\"skills\":[{\"name\":\"elixir\",\"meta\":{\"years\":3}},{\"name\":\"haskell\"}]}",
        "{\"username\":\"duplicate\",\"skills\":[{\"name\":\"dup\"},{\"name\":\"dup\"}]}");
    refreshIndex(INDEX);
  }

  @AfterEach
  public void cleanupAfterEach() throws Exception {
    // Best-effort cleanup for any test-local indices created during tests.
    try {
      deleteIndexIfExists(INDEX + "_not_array");
      deleteIndexIfExists(INDEX + "_missing_field");
    } catch (Exception ignored) {
      // ignore: cleanup best-effort only
    }
  }

  @Test
  public void testMvexpandSingleElement() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='single' | fields username, skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("single", "go"));
  }

  @Test
  public void testMvexpandEmptyArray() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='empty' | fields username, skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result); // expect no rows
  }

  @Test
  public void testMvexpandNullArray() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='nullskills' | fields username,"
                + " skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result); // expect no rows
  }

  @Test
  public void testMvexpandNoArrayField() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='noskills' | fields username,"
                + " skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result); // expect no rows
  }

  @Test
  public void testMvexpandDuplicate() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='duplicate' | fields username,"
                + " skills.name | sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("duplicate", "dup"), rows("duplicate", "dup"));
  }

  /** Verify expansion for 'happy' record (multiple elements). Sort to make assertions stable. */
  @Test
  public void testMvexpandHappyMultipleElements() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='happy' | fields username, skills.name |"
                + " sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    // inside testMvexpandHappyMultipleElements(), after JSONObject result = executeQuery(query);
    System.out.println("DEBUG testMvexpandHappyMultipleElements result: " + result.toString());
    verifyDataRows(result, rows("happy", "java"), rows("happy", "python"), rows("happy", "sql"));
  }

  @Test
  public void testMvexpandPartialElementMissingName() throws Exception {
    // One of the elements does not have the 'name' key and one has explicit null.
    // The expansion should still emit rows for every element; elements missing 'name' => null
    // value.
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='partial' | fields username, skills.name"
                + " | sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    // We expect three rows: one with 'kotlin' and two rows where skills.name is null.
    verifyDataRows(
        result,
        rows("partial", "kotlin"),
        rows("partial", (String) null),
        rows("partial", (String) null));
  }

  @Test
  public void testMvexpandMixedShapesKeepsAllElements() throws Exception {
    // Elements with different internal shapes (additional nested maps) should still be expanded.
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='mixed_shapes' | fields username,"
                + " skills.name | sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    // We expect both elements present after expansion.
    verifyDataRows(result, rows("mixed_shapes", "elixir"), rows("mixed_shapes", "haskell"));
  }

  /**
   * When the field mapping is explicitly a scalar (keyword), the planner/runtime rejects mvexpand
   * with a SemanticCheckException. This test asserts the observable server-side behavior.
   */
  @Test
  public void testMvexpandOnNonArrayFieldMapping() throws Exception {
    final String idx =
        createTempIndexWithMapping(
            INDEX + "_not_array",
            "{ \"mappings\": { \"properties\": { "
                + "\"username\": { \"type\": \"keyword\" },"
                + "\"skills\": { \"type\": \"keyword\" }"
                + "} } }");

    try {
      bulkInsert(idx, "{\"username\":\"u1\",\"skills\":\"scala\"}");
      refreshIndex(idx);

      String query =
          String.format(
              "source=%s | mvexpand skills | where username='u1' | fields username, skills", idx);

      ResponseException ex =
          org.junit.jupiter.api.Assertions.assertThrows(
              ResponseException.class, () -> executeQuery(query));
      String msg = ex.getMessage();
      org.junit.jupiter.api.Assertions.assertTrue(
          msg.contains("Cannot expand field 'skills': expected ARRAY type but found VARCHAR"),
          "Expected SemanticCheckException about non-array field, got: " + msg);
    } finally {
      deleteIndexIfExists(idx);
    }
  }

  /**
   * When the field is missing entirely from the document mapping, mvexpand should not emit rows.
   */
  @Test
  public void testMvexpandMissingFieldReturnsEmpty() throws Exception {
    final String idx =
        createTempIndexWithMapping(
            INDEX + "_missing_field",
            "{ \"mappings\": { \"properties\": { \"username\": { \"type\": \"keyword\" } } } }");

    try {
      bulkInsert(idx, "{\"username\":\"u_missing\"}");
      refreshIndex(idx);

      String query =
          String.format(
              "source=%s | mvexpand skills | where username='u_missing' | fields username, skills",
              idx);

      JSONObject result = executeQuery(query);
      verifyDataRows(result); // expect empty result set for missing field
    } finally {
      deleteIndexIfExists(idx);
    }
  }

  /**
   * Create a transient index with the provided mapping JSON. Caller should delete in a finally
   * block.
   */
  private static String createTempIndexWithMapping(String baseName, String mappingJson)
      throws IOException {
    deleteIndexIfExists(baseName);
    createIndex(baseName, mappingJson);
    return baseName;
  }

  private static void createIndex(String index, String mappingJson) throws IOException {
    Request request = new Request("PUT", "/" + index);
    request.setJsonEntity(mappingJson);
    PPLIntegTestCase.adminClient().performRequest(request);
  }

  /** Delete index if it exists. Swallows IO exceptions to allow best-effort cleanup. */
  private static void deleteIndexIfExists(String index) throws IOException {
    try {
      Request request = new Request("DELETE", "/" + index);
      PPLIntegTestCase.adminClient().performRequest(request);
    } catch (IOException ignored) {
      // ignore, best-effort cleanup
    }
  }

  /**
   * Bulk insert helper: accepts JSON strings. When no id is provided, assigns ascending numeric ids
   * starting at 1.
   */
  private static void bulkInsert(String index, String... docs) throws IOException {
    StringBuilder bulk = new StringBuilder();
    int nextAutoId = 1;
    for (String doc : docs) {
      String id;
      String json;
      if (doc.contains("|")) {
        String[] parts = doc.split("\\|", 2);
        id = parts[0];
        json = parts[1];
      } else {
        id = String.valueOf(nextAutoId++);
        json = doc;
      }
      bulk.append("{\"index\":{\"_id\":").append(id).append("}}\n");
      bulk.append(json).append("\n");
    }
    Request request = new Request("POST", "/" + index + "/_bulk?refresh=true");
    request.setJsonEntity(bulk.toString());
    PPLIntegTestCase.adminClient().performRequest(request);
  }

  private static void refreshIndex(String index) throws IOException {
    Request request = new Request("POST", "/" + index + "/_refresh");
    PPLIntegTestCase.adminClient().performRequest(request);
  }
}

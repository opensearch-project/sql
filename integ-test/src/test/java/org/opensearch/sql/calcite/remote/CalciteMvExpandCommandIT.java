/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.SQLIntegTestCase.Index;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteMvExpandCommandIT extends PPLIntegTestCase {

  private static final String INDEX = Index.MVEXPAND_EDGE_CASES.getName();

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    deleteIndexIfExists(INDEX);
    createIndex(
        INDEX,
        "{ \"mappings\": { \"properties\": { "
            + "\"username\": { \"type\": \"keyword\" },"
            + "\"skills\": { \"type\": \"nested\" }"
            + "} } }");

    // Pass plain JSON documents; bulkInsert will auto-assign incremental ids.
    bulkInsert(
        INDEX,
        "{\"username\":\"happy\",\"skills\":[{\"name\":\"python\"},{\"name\":\"java\"},{\"name\":\"sql\"}]}",
        "{\"username\":\"single\",\"skills\":[{\"name\":\"go\"}]}",
        "{\"username\":\"empty\",\"skills\":[]}",
        "{\"username\":\"nullskills\",\"skills\":null}",
        "{\"username\":\"noskills\"}",
        "{\"username\":\"duplicate\",\"skills\":[{\"name\":\"dup\"},{\"name\":\"dup\"}]}",
        "{\"username\":\"large\",\"skills\":[{\"name\":\"s1\"},{\"name\":\"s2\"},{\"name\":\"s3\"},{\"name\":\"s4\"},{\"name\":\"s5\"},{\"name\":\"s6\"},{\"name\":\"s7\"},{\"name\":\"s8\"},{\"name\":\"s9\"},{\"name\":\"s10\"}]}");
    refreshIndex(INDEX);
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
    verifyDataRows(result); // Should be empty
  }

  @Test
  public void testMvexpandNullArray() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='nullskills' | fields username,"
                + " skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result); // Should be empty
  }

  @Test
  public void testMvexpandNoArrayField() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='noskills' | fields username,"
                + " skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result); // Should be empty
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

  // Helper methods for index setup/teardown
  private static void deleteIndexIfExists(String index) throws IOException {
    try {
      Request request = new Request("DELETE", "/" + index);
      PPLIntegTestCase.adminClient().performRequest(request);
    } catch (IOException e) {
      // Index does not exist or already deleted
    }
  }

  private static void createIndex(String index, String mappingJson) throws IOException {
    Request request = new Request("PUT", "/" + index);
    request.setJsonEntity(mappingJson);
    PPLIntegTestCase.adminClient().performRequest(request);
  }

  /**
   * Bulk insert helper: - Accepts plain JSON strings (no id): assigns incremental numeric ids
   * starting at 1. - Also accepts legacy "id|<json>" strings if a test prefers explicit ids.
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

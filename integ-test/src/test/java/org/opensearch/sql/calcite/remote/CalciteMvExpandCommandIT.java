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
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteMvExpandCommandIT extends PPLIntegTestCase {

  private static final String INDEX = "mvexpand_edge_cases";

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
    bulkInsert(
        INDEX,
        "1|{\"username\":\"happy\","
            + " \"skills\":[{\"name\":\"python\"},{\"name\":\"java\"},{\"name\":\"sql\"}]}",
        "2|{\"username\":\"single\", \"skills\":[{\"name\":\"go\"}]}",
        "3|{\"username\":\"empty\", \"skills\":[]}",
        "4|{\"username\":\"nullskills\", \"skills\":null}",
        "5|{\"username\":\"noskills\"}",
        "6|{\"username\":\"missingattr\", \"skills\":[{\"name\":\"c\"},{\"level\":\"advanced\"}]}",
        "7|{\"username\":\"complex\","
            + " \"skills\":[{\"name\":\"ml\",\"level\":\"expert\"},{\"name\":\"ai\"},{\"level\":\"novice\"}]}",
        "8|{\"username\":\"duplicate\", \"skills\":[{\"name\":\"dup\"},{\"name\":\"dup\"}]}",
        "9|{\"username\":\"large\","
            + " \"skills\":[{\"name\":\"s1\"},{\"name\":\"s2\"},{\"name\":\"s3\"},{\"name\":\"s4\"},{\"name\":\"s5\"},{\"name\":\"s6\"},{\"name\":\"s7\"},{\"name\":\"s8\"},{\"name\":\"s9\"},{\"name\":\"s10\"}]}");
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

  private static void bulkInsert(String index, String... docs) throws IOException {
    StringBuilder bulk = new StringBuilder();
    for (String doc : docs) {
      String[] parts = doc.split("\\|", 2);
      bulk.append("{\"index\":{\"_id\":").append(parts[0]).append("}}\n");
      bulk.append(parts[1]).append("\n");
    }
    Request request = new Request("POST", "/" + index + "/_bulk?refresh=true");
    request.setJsonEntity(bulk.toString());
    PPLIntegTestCase.adminClient().performRequest(request);
  }

  private static void refreshIndex(String index) throws IOException {
    Request request = new Request("POST", "/" + index + "/_refresh");
    PPLIntegTestCase.adminClient().performRequest(request);
  }

  //  @Test
  //  public void testMvexpandComplex() throws Exception {
  //    String query = String.format(
  //            "source=%s | mvexpand skills | where username='complex' | fields username,
  // skills.name, skills.level | sort skills.level",
  //            INDEX);
  //    JSONObject result = executeQuery(query);
  //    verifyDataRows(result,
  //            rows("complex", "ai", null),
  //            rows("complex", "ml", "expert"),
  //            rows("complex", null, "novice")
  //    );
  //  }
  //  @Test
  //  public void testMvexpandLargeArray() throws Exception {
  //    String query = String.format(
  //            "source=%s | mvexpand skills | where username='large' | fields skills.name | sort
  // skills.name",
  //            INDEX);
  //    JSONObject result = executeQuery(query);
  //    verifyDataRows(result,
  //            rows("s1"), rows("s10"), rows("s2"), rows("s3"), rows("s4"),
  //            rows("s5"), rows("s6"), rows("s7"), rows("s8"), rows("s9")
  //    );
  //  }
  //  @Test
  //  public void testMvexpandMissingAttribute() throws Exception {
  //    String query = String.format(
  //            "source=%s | mvexpand skills | where username='missingattr' | fields username,
  // skills.name, skills.level | sort skills.level",
  //            INDEX);
  //    JSONObject result = executeQuery(query);
  //    verifyDataRows(result, rows("missingattr", "c", null), rows("missingattr", null,
  // "advanced"));
  //  }
  //  @Test
  //  public void testMvexpandNormal() throws Exception {
  //    String query = String.format(
  //            "source=%s | mvexpand skills | where username='happy' | fields username, skills.name
  // | sort skills.name",
  //            INDEX);
  //    JSONObject result = executeQuery(query);
  //    verifyDataRows(result, rows("happy", "java"), rows("happy", "python"), rows("happy",
  // "sql"));
  //  }
}

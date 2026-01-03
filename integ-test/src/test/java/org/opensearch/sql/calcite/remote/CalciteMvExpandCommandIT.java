/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

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

    bulkInsert(
        INDEX,
        "{\"username\":\"happy\",\"skills\":[{\"name\":\"python\"},{\"name\":\"java\"},{\"name\":\"sql\"}]}",
        "{\"username\":\"single\",\"skills\":[{\"name\":\"go\"}]}",
        "{\"username\":\"empty\",\"skills\":[]}",
        "{\"username\":\"nullskills\",\"skills\":null}",
        "{\"username\":\"noskills\"}",
        "{\"username\":\"partial\",\"skills\":[{\"name\":\"kotlin\"},{\"level\":\"intern\"},{\"name\":null}]}",
        "{\"username\":\"mixed_shapes\",\"skills\":[{\"name\":\"elixir\",\"meta\":{\"years\":3}},{\"name\":\"haskell\"}]}",
        "{\"username\":\"duplicate\",\"skills\":[{\"name\":\"dup\"},{\"name\":\"dup\"}]}",
        "{\"username\":\"complex\",\"skills\":[{\"name\":\"ml\",\"level\":\"expert\"},{\"name\":\"ai\"},{\"level\":\"novice\"}]}",
        "{\"username\":\"large\",\"skills\":["
            + "{\"name\":\"s1\"},{\"name\":\"s2\"},{\"name\":\"s3\"},{\"name\":\"s4\"},{\"name\":\"s5\"},"
            + "{\"name\":\"s6\"},{\"name\":\"s7\"},{\"name\":\"s8\"},{\"name\":\"s9\"},{\"name\":\"s10\"}"
            + "]}",
        "{\"username\":\"hetero_types\",\"skills\":[{\"level\":\"senior\"},{\"level\":3}]}");

    refreshIndex(INDEX);
  }

  @AfterEach
  public void cleanupAfterEach() throws Exception {
    try {
      deleteIndexIfExists(INDEX + "_not_array");
      deleteIndexIfExists(INDEX + "_missing_field");
      deleteIndexIfExists(INDEX + "_limit_test");
      deleteIndexIfExists(INDEX + "_int_field");
    } catch (Exception ignored) {
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
    verifyDataRows(result);
  }

  @Test
  public void testMvexpandNullArray() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='nullskills' | fields username,"
                + " skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result);
  }

  @Test
  public void testMvexpandNoArrayField() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='noskills' | fields username,"
                + " skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result);
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

  @Test
  public void testMvexpandHappyMultipleElements() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='happy' | fields username, skills.name |"
                + " sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("happy", "java"), rows("happy", "python"), rows("happy", "sql"));
  }

  @Test
  public void testMvexpandPartialElementMissingName() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='partial' | fields username, skills.name"
                + " | sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(
        result,
        rows("partial", "kotlin"),
        rows("partial", (String) null),
        rows("partial", (String) null));
  }

  @Test
  public void testMvexpandMixedShapesKeepsAllElements() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='mixed_shapes' | fields username,"
                + " skills.name | sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("mixed_shapes", "elixir"), rows("mixed_shapes", "haskell"));
  }

  @Test
  public void testMvexpandFlattenedSchemaPresence() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='complex' | fields username,"
                + " skills.level, skills.name",
            INDEX);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("username", "string"),
        schema("skills.level", "string"),
        schema("skills.name", "string"));

    verifyDataRows(
        result,
        rows("complex", "expert", "ml"),
        rows("complex", (String) null, "ai"),
        rows("complex", "novice", (String) null));
  }

  @Test
  public void testMvexpandOnNonArrayFieldMapping() throws Exception {
    String idx = Index.MVEXPAND_NOT_ARRAY.getName();

    bulkInsert(idx, "{\"username\":\"u1\",\"skills\":\"scala\"}");
    refreshIndex(idx);

    String query =
        String.format(
            "source=%s | mvexpand skills | where username='u1' | fields username, skills", idx);

    ResponseException ex = assertThrows(ResponseException.class, () -> executeQuery(query));
    Assertions.assertTrue(
        ex.getMessage()
            .contains("Cannot expand field 'skills': expected ARRAY type but found VARCHAR"));
  }

  //  @Test
  //  public void testMvexpandOnNonArrayFieldMapping() throws Exception {
  //    final String idx =
  //        createTempIndexWithMapping(
  //            INDEX + "_not_array",
  //            "{ \"mappings\": { \"properties\": { "
  //                + "\"username\": { \"type\": \"keyword\" },"
  //                + "\"skills\": { \"type\": \"keyword\" }"
  //                + "} } }");
  //
  //    bulkInsert(idx, "{\"username\":\"u1\",\"skills\":\"scala\"}");
  //    refreshIndex(idx);
  //
  //    String query =
  //        String.format(
  //            "source=%s | mvexpand skills | where username='u1' | fields username, skills", idx);
  //
  //    ResponseException ex = assertThrows(ResponseException.class, () -> executeQuery(query));
  //    String msg = ex.getMessage();
  //    Assertions.assertTrue(
  //        msg.contains("Cannot expand field 'skills': expected ARRAY type but found VARCHAR"),
  //        "Expected SemanticCheckException about non-array field, got: " + msg);
  //  }

  @Test
  public void testMvexpandMissingFieldReturnsEmpty() throws Exception {
    String idx = Index.MVEXPAND_MISSING_FIELD.getName();

    bulkInsert(idx, "{\"username\":\"u_missing\"}");
    refreshIndex(idx);

    String query =
        String.format(
            "source=%s | mvexpand skills | where username='u_missing' | fields username, skills",
            idx);

    JSONObject result = executeQuery(query);
    verifyDataRows(result);
  }

  //  @Test
  //  public void testMvexpandMissingFieldReturnsEmpty() throws Exception {
  //    final String idx =
  //        createTempIndexWithMapping(
  //            INDEX + "_missing_field",
  //            "{ \"mappings\": { \"properties\": { \"username\": { \"type\": \"keyword\" } } }
  // }");
  //
  //    bulkInsert(idx, "{\"username\":\"u_missing\"}");
  //    refreshIndex(idx);
  //
  //    String query =
  //        String.format(
  //            "source=%s | mvexpand skills | where username='u_missing' | fields username,
  // skills",
  //            idx);
  //
  //    JSONObject result = executeQuery(query);
  //    verifyDataRows(result);
  //  }

  @Test
  public void testMvexpandLimitParameter() throws Exception {
    final String idx = INDEX + "_limit_test";
    deleteIndexIfExists(idx);
    createIndex(
        idx,
        "{ \"mappings\": { \"properties\": { \"username\": { \"type\": \"keyword\" },"
            + "\"skills\": { \"type\": \"nested\" } } } }");

    bulkInsert(
        idx,
        "{\"username\":\"limituser\",\"skills\":["
            + "{\"name\":\"a\"},{\"name\":\"b\"},{\"name\":\"c\"},{\"name\":\"d\"},{\"name\":\"e\"}"
            + "]}");
    refreshIndex(idx);

    String query =
        String.format(
            "source=%s | mvexpand skills limit=3 | where username='limituser' | fields username,"
                + " skills.name",
            idx);
    JSONObject result = executeQuery(query);
    verifyNumOfRows(result, 3);
    verifyDataRows(result, rows("limituser", "a"), rows("limituser", "b"), rows("limituser", "c"));
  }

  @Test
  public void testMvexpandTypeInferenceForHeterogeneousSubfields() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='hetero_types' | fields username,"
                + " skills.level",
            INDEX);
    JSONObject result = executeQuery(query);

    verifyDataRows(result, rows("hetero_types", "senior"), rows("hetero_types", "3"));
  }

  @Test
  public void testMvexpandLargeArrayElements() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='large' | fields username, skills.name |"
                + " sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);

    verifyNumOfRows(result, 10);

    verifyDataRows(
        result,
        rows("large", "s1"),
        rows("large", "s2"),
        rows("large", "s3"),
        rows("large", "s4"),
        rows("large", "s5"),
        rows("large", "s6"),
        rows("large", "s7"),
        rows("large", "s8"),
        rows("large", "s9"),
        rows("large", "s10"));
  }

  @Test
  public void testMvexpandOnIntegerFieldMappingThrowsSemantic() throws Exception {
    String idx = Index.MVEXPAND_INT_FIELD.getName();

    bulkInsert(idx, "{\"username\":\"u_int\",\"skills\":5}");
    refreshIndex(idx);

    String query =
        String.format(
            "source=%s | mvexpand skills | where username='u_int' | fields username, skills", idx);

    ResponseException ex = assertThrows(ResponseException.class, () -> executeQuery(query));
    Assertions.assertTrue(
        ex.getMessage().contains("Cannot expand field") || ex.getMessage().contains("Semantic"),
        "Expected semantic error for non-array field, got: " + ex.getMessage());
  }

  //  @Test
  //  public void testMvexpandOnIntegerFieldMappingThrowsSemantic() throws Exception {
  //    // Verify mvexpand raises a semantic error when the target field is mapped as a non-array
  //    // numeric type (e.g. integer). This exercises the code branch that checks the resolved
  //    // RexInputRef against the current row type and throws SemanticCheckException.
  //    final String idx =
  //        createTempIndexWithMapping(
  //            INDEX + "_int_field",
  //            "{ \"mappings\": { \"properties\": { "
  //                + "\"username\": { \"type\": \"keyword\" },"
  //                + "\"skills\": { \"type\": \"integer\" }"
  //                + "} } }");
  //    bulkInsert(idx, "{\"username\":\"u_int\",\"skills\":5}");
  //    refreshIndex(idx);
  //
  //    String query =
  //        String.format(
  //            "source=%s | mvexpand skills | where username='u_int' | fields username, skills",
  // idx);
  //
  //    ResponseException ex = assertThrows(ResponseException.class, () -> executeQuery(query));
  //    String msg = ex.getMessage();
  //    Assertions.assertTrue(
  //        msg.contains("Cannot expand field 'skills': expected ARRAY type but found INTEGER"),
  //        "Expected SemanticCheckException about non-array integer field, got: " + msg);
  //  }

  //  private static String createTempIndexWithMapping(String baseName, String mappingJson)
  //      throws IOException {
  //    deleteIndexIfExists(baseName);
  //    createIndex(baseName, mappingJson);
  //    return baseName;
  //  }

  private static void createIndex(String index, String mappingJson) throws IOException {
    Request request = new Request("PUT", "/" + index);
    request.setJsonEntity(mappingJson);
    PPLIntegTestCase.adminClient().performRequest(request);
  }

  private static void deleteIndexIfExists(String index) throws IOException {
    try {
      Request request = new Request("DELETE", "/" + index);
      PPLIntegTestCase.adminClient().performRequest(request);
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

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

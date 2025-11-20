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
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for mvexpand behavior via Calcite translation.
 *
 * <p>This test follows the layout and style of CalciteExpandCommandIT but targets the mvexpand
 * command semantics and edge cases. The canonical fixture created in init() contains documents used
 * by the tests. Per-test temporary indices are created for mapping-specific edge cases to keep
 * tests deterministic and isolated.
 */
public class CalciteMvExpandCommandIT extends PPLIntegTestCase {

  private static final String INDEX = Index.MVEXPAND_EDGE_CASES.getName();

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    deleteIndexIfExists(INDEX);

    // Use nested mapping so that element sub-fields can be flattened into separate columns.
    final String nestedMapping =
        "{ \"mappings\": { \"properties\": { "
            + "\"username\": { \"type\": \"keyword\" },"
            + "\"skills\": { \"type\": \"nested\" }"
            + "} } }";

    createIndex(INDEX, nestedMapping);

    // Canonical fixture documents: include records asserted by tests.
    bulkInsert(
        INDEX,
        // happy: multiple elements with only 'name'
        "{\"username\":\"happy\",\"skills\":[{\"name\":\"python\"},{\"name\":\"java\"},{\"name\":\"sql\"}]}",
        // single: single-element array
        "{\"username\":\"single\",\"skills\":[{\"name\":\"go\"}]}",
        // empty: empty array
        "{\"username\":\"empty\",\"skills\":[]}",
        // nullskills: null value
        "{\"username\":\"nullskills\",\"skills\":null}",
        // noskills: no skills field at all
        "{\"username\":\"noskills\"}",
        // partial: some elements missing 'name' or explicitly null
        "{\"username\":\"partial\",\"skills\":[{\"name\":\"kotlin\"},{\"level\":\"intern\"},{\"name\":null}]}",
        // mixed_shapes: elements with additional nested maps
        "{\"username\":\"mixed_shapes\",\"skills\":[{\"name\":\"elixir\",\"meta\":{\"years\":3}},{\"name\":\"haskell\"}]}",
        // duplicate: duplicated elements preserved
        "{\"username\":\"duplicate\",\"skills\":[{\"name\":\"dup\"},{\"name\":\"dup\"}]}",
        // complex: elements where some have both fields, some missing, used to assert flattening
        "{\"username\":\"complex\",\"skills\":[{\"name\":\"ml\",\"level\":\"expert\"},{\"name\":\"ai\"},{\"level\":\"novice\"}]}",
        // large: many elements to exercise multiple rows generation
        "{\"username\":\"large\",\"skills\":["
            + "{\"name\":\"s1\"},{\"name\":\"s2\"},{\"name\":\"s3\"},{\"name\":\"s4\"},{\"name\":\"s5\"},"
            + "{\"name\":\"s6\"},{\"name\":\"s7\"},{\"name\":\"s8\"},{\"name\":\"s9\"},{\"name\":\"s10\"}"
            + "]}",
        // hetero_types: same sub-field 'level' as number and string to check type inference edge
        // case
        "{\"username\":\"hetero_types\",\"skills\":[{\"level\":\"senior\"},{\"level\":3}]}");

    // Make indexed documents available for search
    refreshIndex(INDEX);
  }

  @AfterEach
  public void cleanupAfterEach() throws Exception {
    // best-effort cleanup for test-local indices
    try {
      deleteIndexIfExists(INDEX + "_not_array");
      deleteIndexIfExists(INDEX + "_missing_field");
      deleteIndexIfExists(INDEX + "_limit_test");
      deleteIndexIfExists(INDEX + "_int_field");
    } catch (Exception ignored) {
      // ignore
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
    // Expect three rows: kotlin, null, null (two elements missing name or name==null)
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
    // Verify that when sub-fields exist they are exposed as flattened columns.
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='complex' | fields username,"
                + " skills.level, skills.name",
            INDEX);
    JSONObject result = executeQuery(query);

    // Schema should contain flattened columns for skills.level and skills.name
    verifySchema(
        result,
        schema("username", "string"),
        schema("skills.level", "string"),
        schema("skills.name", "string"));

    // Verify rows (order not important here)
    verifyDataRows(
        result,
        rows("complex", "expert", "ml"),
        rows("complex", (String) null, "ai"),
        rows("complex", "novice", (String) null));
  }

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

  @Test
  public void testMvexpandLimitParameter() throws Exception {
    // Create a small index to test limit parameter semantics deterministically
    final String idx = INDEX + "_limit_test";
    deleteIndexIfExists(idx);
    createIndex(
        idx,
        "{ \"mappings\": { \"properties\": { \"username\": { \"type\": \"keyword\" },"
            + "\"skills\": { \"type\": \"nested\" } } } }");

    try {
      // single document with many elements
      bulkInsert(
          idx,
          "{\"username\":\"limituser\",\"skills\":["
              + "{\"name\":\"a\"},{\"name\":\"b\"},{\"name\":\"c\"},{\"name\":\"d\"},{\"name\":\"e\"}"
              + "]}");
      refreshIndex(idx);

      // mvexpand with limit=3 should produce only 3 rows for that document
      String query =
          String.format(
              "source=%s | mvexpand skills limit=3 | where username='limituser' | fields username,"
                  + " skills.name",
              idx);
      JSONObject result = executeQuery(query);
      verifyNumOfRows(result, 3);
      verifyDataRows(
          result, rows("limituser", "a"), rows("limituser", "b"), rows("limituser", "c"));
    } finally {
      deleteIndexIfExists(idx);
    }
  }

  @Test
  public void testMvexpandTypeInferenceForHeterogeneousSubfields() throws Exception {
    // Some elements have 'level' as string and some as number. The system should still expand rows,
    // but the reported schema type may be "undefined" or a common supertype.
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='hetero_types' | fields username,"
                + " skills.level",
            INDEX);
    JSONObject result = executeQuery(query);

    // Should produce two rows (one with "senior", one with 3)
    verifyDataRows(result, rows("hetero_types", "senior"), rows("hetero_types", "3"));
  }

  @Test
  public void testMvexpandLargeArrayElements() throws Exception {
    // Verify that a document with 10 elements expands into 10 rows and that all element names are
    // present.
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='large' | fields username, skills.name |"
                + " sort skills.name",
            INDEX);
    JSONObject result = executeQuery(query);

    // Expect 10 rows (s1..s10)
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
    // Verify mvexpand raises a semantic error when the target field is mapped as a non-array
    // numeric type (e.g. integer). This exercises the code branch that checks the resolved
    // RexInputRef against the current row type and throws SemanticCheckException.
    final String idx =
        createTempIndexWithMapping(
            INDEX + "_int_field",
            "{ \"mappings\": { \"properties\": { "
                + "\"username\": { \"type\": \"keyword\" },"
                + "\"skills\": { \"type\": \"integer\" }"
                + "} } }");
    try {
      bulkInsert(idx, "{\"username\":\"u_int\",\"skills\":5}");
      refreshIndex(idx);

      String query =
          String.format(
              "source=%s | mvexpand skills | where username='u_int' | fields username, skills",
              idx);

      ResponseException ex =
          org.junit.jupiter.api.Assertions.assertThrows(
              ResponseException.class, () -> executeQuery(query));
      String msg = ex.getMessage();
      org.junit.jupiter.api.Assertions.assertTrue(
          msg.contains("Cannot expand field 'skills': expected ARRAY type but found INTEGER"),
          "Expected SemanticCheckException about non-array integer field, got: " + msg);
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

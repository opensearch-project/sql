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

import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteMvExpandCommandIT extends PPLIntegTestCase {

  private static final String INDEX = Index.MVEXPAND_EDGE_CASES.getName();

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.MVEXPAND_EDGE_CASES);
  }

  @Test
  public void testMvexpandSingleElement() throws Exception {
    String q1 =
        String.format(
            "source=%s | mvexpand skills | where username='single' | fields username, skills",
            INDEX);
    JSONObject r1 = executeQuery(q1);

    assertSingleRowNestedFieldEquals(r1, "skills", "name", "go");

    String q2 =
        String.format(
            "source=%s | mvexpand skills | where username='single' | fields username, skills.name",
            INDEX);
    JSONObject r2 = executeQuery(q2);
    verifyDataRows(r2, rows("single", "go"));
  }

  /**
   * Asserts the result has exactly one row and that the given column is a MAP/object containing
   * nestedKey=nestedValue.
   */
  private static void assertSingleRowNestedFieldEquals(
      JSONObject result, String mapColumn, String nestedKey, String expectedValue) {
    var dataRows = result.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Expected exactly one row");

    var schema = result.getJSONArray("schema");

    int mapIdx = -1;
    for (int i = 0; i < schema.length(); i++) {
      if (mapColumn.equals(schema.getJSONObject(i).getString("name"))) {
        mapIdx = i;
        break;
      }
    }
    Assertions.assertTrue(mapIdx >= 0, "Column not found in schema: " + mapColumn);

    var row0 = dataRows.getJSONArray(0);
    var skillsObj = row0.getJSONObject(mapIdx); // this is the MAP/object
    Assertions.assertEquals(expectedValue, skillsObj.optString(nestedKey, null));
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
    String query =
        String.format(
            "source=%s | mvexpand skills_not_array | where username='u1' | fields username,"
                + " skills_not_array",
            INDEX);

    JSONObject result = executeQuery(query);

    verifyNumOfRows(result, 1);
    verifyDataRows(result, rows("u1", "scala"));
  }

  @Test
  public void testMvexpandMissingFieldReturnsEmpty() throws Exception {
    // single-index version: username='noskills' doc has no "skills" field at all
    String query =
        String.format(
            "source=%s | mvexpand skills | where username='noskills' | fields username, skills",
            INDEX);

    JSONObject result = executeQuery(query);
    verifyDataRows(result);
  }

  @Test
  public void testMvexpandLimitParameter() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills limit=3 | where username='limituser' | fields username,"
                + " skills.name",
            INDEX);
    JSONObject result = executeQuery(query);
    verifyNumOfRows(result, 3);
    verifyDataRows(result, rows("limituser", "a"), rows("limituser", "b"), rows("limituser", "c"));
  }

  @Test
  public void testMvexpandMultiDocumentLimitParameter() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills limit=2 | where username='happy' OR username='limituser'"
                + " | fields username, skills.name | sort username, skills.name",
            INDEX);
    JSONObject result = executeQuery(query);

    verifyNumOfRows(result, 4);

    verifyDataRows(
        result,
        rows("happy", "java"),
        rows("happy", "python"),
        rows("limituser", "a"),
        rows("limituser", "b"));
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
        rows("large", "s10"),
        rows("large", "s2"),
        rows("large", "s3"),
        rows("large", "s4"),
        rows("large", "s5"),
        rows("large", "s6"),
        rows("large", "s7"),
        rows("large", "s8"),
        rows("large", "s9"));
  }

  @Test
  public void testMvexpandOnIntegerFieldMapping() throws Exception {
    String query =
        String.format(
            "source=%s | mvexpand skills_int | where username='u_int' | fields username,"
                + " skills_int",
            INDEX);

    JSONObject result = executeQuery(query);

    verifyNumOfRows(result, 1);
    verifyDataRows(result, rows("u_int", 5));
  }
}

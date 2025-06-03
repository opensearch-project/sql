/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WORKER;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WORK_INFORMATION;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;

public class CalcitePPLLookupIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.WORKER);
    loadIndex(Index.WORK_INFORMATION);
  }

  @Test
  public void testUidAsIdReplaceDepartment() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s| LOOKUP %s uid AS id REPLACE department| fields id, name, occupation, country, salary, department",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("occupation", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("department", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "Engineer", "England", 100000, "IT"),
        rows(1001, "Hello", "Artist", "USA", 70000, null),
        rows(1002, "John", "Doctor", "Canada", 120000, "DATA"),
        rows(1003, "David", "Doctor", null, 120000, "HR"),
        rows(1004, "David", null, "Canada", 0, null),
        rows(1005, "Jane", "Scientist", "Canada", 90000, "DATA"));
  }

  @Test
  public void testUidAsIdAppendDepartment() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | LOOKUP %s uid AS id APPEND department| fields id, name, occupation, country, salary, department",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("occupation", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("department", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "Engineer", "England", 100000, "IT"),
        rows(1001, "Hello", "Artist", "USA", 70000, null),
        rows(1002, "John", "Doctor", "Canada", 120000, "DATA"),
        rows(1003, "David", "Doctor", null, 120000, "HR"),
        rows(1004, "David", null, "Canada", 0, null),
        rows(1005, "Jane", "Scientist", "Canada", 90000, "DATA"));
  }

  @Test
  public void testUidAsIdReplaceDepartmentAsCountry() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | LOOKUP %s uid AS id REPLACE department AS country| fields id, name, occupation, salary, country",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("occupation", "string"),
        schema("salary", "integer"),
        schema("country", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "Engineer", 100000, "IT"),
        rows(1001, "Hello", "Artist", 70000, null),
        rows(1002, "John", "Doctor", 120000, "DATA"),
        rows(1003, "David", "Doctor", 120000, "HR"),
        rows(1004, "David", null, 0, null),
        rows(1005, "Jane", "Scientist", 90000, "DATA"));
  }

  @Test
  public void testUidAsIdAppendDepartmentAsCountry() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s| LOOKUP %s uid AS id APPEND department AS country| fields id, name, occupation, salary, country",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("occupation", "string"),
        schema("salary", "integer"),
        schema("country", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "Engineer", 100000, "England"),
        rows(1001, "Hello", "Artist", 70000, "USA"),
        rows(1002, "John", "Doctor", 120000, "Canada"),
        rows(1003, "David", "Doctor", 120000, "HR"),
        rows(1004, "David", null, 0, "Canada"),
        rows(1005, "Jane", "Scientist", 90000, "Canada"));
  }

  @Test
  public void testUidAsIdNameReplaceDepartment() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s| LOOKUP %s uid AS id, name REPLACE department | fields id, name, occupation, country, salary, department",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("occupation", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("department", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "Engineer", "England", 100000, "IT"),
        rows(1001, "Hello", "Artist", "USA", 70000, null),
        rows(1002, "John", "Doctor", "Canada", 120000, "DATA"),
        rows(1003, "David", "Doctor", null, 120000, "HR"),
        rows(1004, "David", null, "Canada", 0, null),
        rows(1005, "Jane", "Scientist", "Canada", 90000, "DATA"));
  }

  @Test
  public void testUidAsIdNameAppendDepartment() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s| LOOKUP %s uid AS id, name APPEND department| fields id, name, occupation, country, salary, department ",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("occupation", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("department", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "Engineer", "England", 100000, "IT"),
        rows(1001, "Hello", "Artist", "USA", 70000, null),
        rows(1002, "John", "Doctor", "Canada", 120000, "DATA"),
        rows(1003, "David", "Doctor", null, 120000, "HR"),
        rows(1004, "David", null, "Canada", 0, null),
        rows(1005, "Jane", "Scientist", "Canada", 90000, "DATA"));
  }

  @Test
  public void testUidASIdName() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | LOOKUP %s uid AS id, name
                   | fields id, name, country, salary, department, occupation
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("department", "string"),
        schema("occupation", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "England", 100000, "IT", "Engineer"),
        rows(1001, "Hello", "USA", 70000, null, null),
        rows(1002, "John", "Canada", 120000, "DATA", "Scientist"),
        rows(1003, "David", null, 120000, "HR", "Doctor"),
        rows(1004, "David", "Canada", 0, null, null),
        rows(1005, "Jane", "Canada", 90000, "DATA", "Engineer"));
  }

  @Test
  public void testNameReplaceOccupationAsMajor() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | eval major = occupation | fields id, name, major, country, salary | LOOKUP %s name REPLACE occupation AS major",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("major", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "England", 100000, "Engineer"),
        rows(1001, "Hello", "USA", 70000, null),
        rows(1002, "John", "Canada", 120000, "Scientist"),
        rows(1003, "David", null, 120000, "Doctor"),
        rows(1004, "David", "Canada", 0, "Doctor"),
        rows(1005, "Jane", "Canada", 90000, "Engineer"));
  }

  @Test
  public void testNameAppendOccupationAsMajor() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | eval major = occupation | fields id, name, major, country, salary | LOOKUP %s name APPEND occupation AS major",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("major", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "England", 100000, "Engineer"),
        rows(1001, "Hello", "USA", 70000, "Artist"),
        rows(1002, "John", "Canada", 120000, "Doctor"),
        rows(1003, "David", null, 120000, "Doctor"),
        rows(1004, "David", "Canada", 0, "Doctor"),
        rows(1005, "Jane", "Canada", 90000, "Scientist"));
  }

  @Test
  public void testName() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | LOOKUP %s name | fields id, name, country, salary, uid, department, occupation",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("uid", "integer"),
        schema("department", "string"),
        schema("occupation", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "England", 100000, 1000, "IT", "Engineer"),
        rows(1001, "Hello", "USA", 70000, null, null, null),
        rows(1002, "John", "Canada", 120000, 1002, "DATA", "Scientist"),
        rows(1003, "David", null, 120000, 1003, "HR", "Doctor"),
        rows(1004, "David", "Canada", 0, 1003, "HR", "Doctor"),
        rows(1005, "Jane", "Canada", 90000, 1005, "DATA", "Engineer"));
  }

  @Test
  public void testIdWithRename() {
    // rename country to department for verify the case if search side is not a table
    // and its output has diffed from the original fields of source table
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s| rename id as uid | rename country as department | LOOKUP %s uid| fields salary, uid, name, department, occupation",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("salary", "integer"),
        schema("uid", "integer"),
        schema("name", "string"),
        schema("department", "string"),
        schema("occupation", "string"));
    verifyDataRows(
        result,
        rows(100000, 1000, "Jake", "IT", "Engineer"),
        rows(70000, 1001, null, null, null),
        rows(120000, 1002, "John", "DATA", "Scientist"),
        rows(120000, 1003, "David", "HR", "Doctor"),
        rows(0, 1004, null, null, null),
        rows(90000, 1005, "Jane", "DATA", "Engineer"));
  }

  @Test
  public void testCorrectnessCombination() throws IOException {
    Request requestS = new Request("PUT", "/s/_doc/1?refresh=true");
    requestS.setJsonEntity("{\"id\": 1, \"col1\": \"a\", \"col2\": \"b\"}");
    client().performRequest(requestS);
    requestS = new Request("PUT", "/s/_doc/2?refresh=true");
    requestS.setJsonEntity("{\"id\": 2, \"col1\": \"aa\", \"col2\": \"bb\"}");
    client().performRequest(requestS);
    requestS = new Request("PUT", "/s/_doc/3?refresh=true");
    requestS.setJsonEntity("{\"id\": 3, \"col1\": null, \"col2\": \"ccc\"}");
    client().performRequest(requestS);

    Request requestL = new Request("PUT", "/l/_doc/1?refresh=true");
    requestL.setJsonEntity("{\"id\": 1, \"col1\": \"x\", \"col3\": \"y\"}");
    client().performRequest(requestL);
    requestL = new Request("PUT", "/l/_doc/3?refresh=true");
    requestL.setJsonEntity("{\"id\": 3, \"col1\": \"xx\", \"col3\": \"yy\"}");
    client().performRequest(requestL);

    JSONObject result = executeQuery("source = s | LOOKUP l id | fields id, col1, col2, col3");
    verifyDataRows(
        result, rows(1, "x", "b", "y"), rows(2, null, "bb", null), rows(3, "xx", "ccc", "yy"));

    result =
        executeQuery(
            "source = s | LOOKUP l id REPLACE id, col1, col3 | fields id, col1, col2, col3");
    verifyDataRows(
        result, rows(1, "x", "b", "y"), rows(null, null, "bb", null), rows(3, "xx", "ccc", "yy"));
    result =
        executeQuery(
            "source = s | LOOKUP l id APPEND id, col1, col3 | fields id, col1, col2, col3");
    verifyDataRows(
        result, rows(1, "a", "b", "y"), rows(2, "aa", "bb", null), rows(3, "xx", "ccc", "yy"));
    result = executeQuery("source = s | LOOKUP l id REPLACE col1 | fields id, col1, col2");
    verifyDataRows(result, rows(1, "x", "b"), rows(2, null, "bb"), rows(3, "xx", "ccc"));
    result = executeQuery("source = s | LOOKUP l id APPEND col1 | fields id, col1, col2");
    verifyDataRows(result, rows(1, "a", "b"), rows(2, "aa", "bb"), rows(3, "xx", "ccc"));
    result = executeQuery("source = s | LOOKUP l id REPLACE col1 as col2 | fields id, col1, col2");
    verifyDataRows(result, rows(1, "a", "x"), rows(2, "aa", null), rows(3, null, "xx"));
    result = executeQuery("source = s | LOOKUP l id APPEND col1 as col2 | fields id, col1, col2");
    verifyDataRows(result, rows(1, "a", "b"), rows(2, "aa", "bb"), rows(3, null, "ccc"));
    result =
        executeQuery("source = s | LOOKUP l id REPLACE col1 as colA | fields id, col1, col2, colA");
    verifyDataRows(
        result, rows(1, "a", "b", "x"), rows(2, "aa", "bb", null), rows(3, null, "ccc", "xx"));
    // source = s | LOOKUP l id APPEND col1 as colA | fields id, col1, col2, colA throw exception
  }

  @Test
  public void testNameReplaceOccupation2() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | LOOKUP %s name REPLACE occupation| fields id, name, country, salary, occupation",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("occupation", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "England", 100000, "Engineer"),
        rows(1001, "Hello", "USA", 70000, null),
        rows(1002, "John", "Canada", 120000, "Scientist"),
        rows(1003, "David", null, 120000, "Doctor"),
        rows(1004, "David", "Canada", 0, "Doctor"),
        rows(1005, "Jane", "Canada", 90000, "Engineer"));
  }

  @Test
  public void testNameReplaceOccupationAsNewName() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | LOOKUP %s name REPLACE occupation AS new_col| fields id, name, occupation, country, salary, new_col",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result,
        schema("id", "integer"),
        schema("name", "string"),
        schema("occupation", "string"),
        schema("country", "string"),
        schema("salary", "integer"),
        schema("new_col", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake", "Engineer", "England", 100000, "Engineer"),
        rows(1001, "Hello", "Artist", "USA", 70000, null),
        rows(1002, "John", "Doctor", "Canada", 120000, "Scientist"),
        rows(1003, "David", "Doctor", null, 120000, "Doctor"),
        rows(1004, "David", null, "Canada", 0, "Doctor"),
        rows(1005, "Jane", "Scientist", "Canada", 90000, "Engineer"));
  }

  @Test
  public void testRnameAsIdShouldnWork() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | eval uid =  id | LOOKUP %s uid as id REPLACE occupation as new_col",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 6);
  }
}

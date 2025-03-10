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
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

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
                """
                   source = %s
                   | LOOKUP %s uid AS id REPLACE department
                   | fields id, name, occupation, country, salary, department
                   """,
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
                """
                   source = %s
                   | LOOKUP %s uid AS id APPEND department
                   | fields id, name, occupation, country, salary, department
                   """,
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
                """
                   source = %s
                   | LOOKUP %s uid AS id REPLACE department AS country
                   | fields id, name, occupation, salary, country
                   """,
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
                """
                   source = %s
                   | LOOKUP %s uid AS id APPEND department AS country
                   | fields id, name, occupation, salary, country
                   """,
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
                """
                   source = %s
                   | LOOKUP %s uID AS id, name REPLACE department
                   | fields id, name, occupation, country, salary, department
                   """,
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
                """
                   source = %s
                   | LOOKUP %s uID AS id, name APPEND department
                   | fields id, name, occupation, country, salary, department
                   """,
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
}

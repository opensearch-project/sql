/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OCCUPATION;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WORKER;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WORK_INFORMATION;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;

public class CalcitePPLExistsSubqueryIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.WORKER);
    loadIndex(Index.WORK_INFORMATION);
    loadIndex(Index.OCCUPATION);

    // {"index":{"_id":"7"}}
    // {"id":1006,"name":"Tommy","occupation":"Teacher","country":"USA","salary":30000}
    Request request1 = new Request("PUT", "/" + TEST_INDEX_WORKER + "/_doc/7?refresh=true");
    request1.setJsonEntity(
        "{\"id\":1006,\"name\":\"Tommy\",\"occupation\":\"Teacher\",\"country\":\"USA\",\"salary\":30000}");
    client().performRequest(request1);
  }

  @Test
  public void testSimpleExistsSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where exists [\n" +
                    "    source = %s | where id = uid\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void testExistsSubqueryAndAggregation() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where exists [
                       source = %s | where id = uid
                     ]
                   | stats count() by country
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("country", "string"), schema("count()", "long"));
    verifyDataRows(result, rows(1, null), rows(2, "Canada"), rows(1, "USA"), rows(1, "England"));
  }

  @Test
  public void testSimpleExistsSubqueryInFilter() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s exists [\n" +
                    "    source = %s | where id = uid\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void testNotExistsSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where not exists [\n" +
                    "    source = %s | where id = uid\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(result, rows(1001, "Hello", 70000), rows(1004, "David", 0));
  }

  @Test
  public void testNotExistsSubqueryInFilter() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s not exists [\n" +
                    "    source = %s | where id = uid\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(result, rows(1001, "Hello", 70000), rows(1004, "David", 0));
  }

  @Test
  public void testEmptyExistsSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where exists [\n" +
                    "    source = %s | where uid = 0000 AND id = uid\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyNumOfRows(result, 0);
  }

  @Test
  public void testUncorrelatedExistsSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where exists [\n" +
                    "    source = %s | where name = 'Tom'\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyNumOfRows(result, 7);

    result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where not exists [\n" +
                    "    source = %s | where name = 'Tom'\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 0);
  }

  @Test
  public void testUncorrelatedExistsSubqueryCheckTheReturnContentOfInnerTableIsEmptyOrNot() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where exists [\n" +
                    "    source = %s\n" +
                    "  ]\n" +
                    "| eval constant = \"Bala\"\n" +
                    "| fields constant\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));

    verifyDataRows(
        result,
        rows("Bala"),
        rows("Bala"),
        rows("Bala"),
        rows("Bala"),
        rows("Bala"),
        rows("Bala"),
        rows("Bala"));

    result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where exists [\n" +
                    "    source = %s | where uid = 999\n" +
                    "  ]\n" +
                    "| eval constant = \"Bala\"\n" +
                    "| fields constant\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 0);
  }

  @Test
  public void testNestedExistsSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where exists [\n" +
                    "    source = %s\n" +
                    "    | where exists [\n" +
                    "        source = %s\n" +
                    "        | where %s.occupation = %s.occupation\n" +
                    "      ]\n" +
                    "    | where id = uid\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER,
                TEST_INDEX_WORK_INFORMATION,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void testExistsSubqueryWithConjunction() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where exists [\n" +
                    "    source = %s\n" +
                    "    | where id = uid AND\n" +
                    "      %s.name = %s.name AND\n" +
                    "      %s.occupation = %s.occupation\n" +
                    "  ]\n" +
                    "| sort  - salary\n" +
                    "| fields id, name, salary\n",
                TEST_INDEX_WORKER,
                TEST_INDEX_WORK_INFORMATION,
                TEST_INDEX_WORKER,
                TEST_INDEX_WORK_INFORMATION,
                TEST_INDEX_WORKER,
                TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(result, rows(1003, "David", 120000), rows(1000, "Jake", 100000));
  }
}

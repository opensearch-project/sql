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
                """
                   source = %s
                   | where exists [
                       source = %s | where id = uid
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
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
  public void testSimpleExistsSubqueryInFilter() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s exists [
                       source = %s | where id = uid
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
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
                """
                   source = %s
                   | where not exists [
                       source = %s | where id = uid
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
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
                """
                   source = %s not exists [
                       source = %s | where id = uid
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
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
                """
                   source = %s
                   | where exists [
                       source = %s | where uid = 0000 AND id = uid
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
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
                """
                   source = %s
                   | where exists [
                       source = %s | where name = 'Tom'
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyNumOfRows(result, 7);

    result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where not exists [
                       source = %s | where name = 'Tom'
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 0);
  }

  @Test
  public void testUncorrelatedExistsSubqueryCheckTheReturnContentOfInnerTableIsEmptyOrNot() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where exists [
                       source = %s
                     ]
                   | eval constant = "Bala"
                   | fields constant
                   """,
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
                """
                   source = %s
                   | where exists [
                       source = %s | where uid = 999
                     ]
                   | eval constant = "Bala"
                   | fields constant
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 0);
  }

  @Test
  public void testNestedExistsSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where exists [
                       source = %s
                       | where exists [
                           source = %s
                           | where %s.occupation = %s.occupation
                         ]
                       | where id = uid
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
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
                """
                   source = %s
                   | where exists [
                       source = %s
                       | where id = uid AND
                         %s.name = %s.name AND
                         %s.occupation = %s.occupation
                     ]
                   | sort  - salary
                   | fields id, name, salary
                   """,
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

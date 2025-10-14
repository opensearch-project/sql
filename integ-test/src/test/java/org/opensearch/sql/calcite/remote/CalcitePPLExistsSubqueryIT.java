/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OCCUPATION;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WORKER;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WORK_INFORMATION;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLExistsSubqueryIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

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
  public void testSimpleExistsSubquery() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void testExistsSubqueryAndAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| stats count() by country",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("country", "string"), schema("count()", "bigint"));
    verifyDataRows(result, rows(1, null), rows(2, "Canada"), rows(1, "USA"), rows(1, "England"));
  }

  @Test
  public void testSimpleExistsSubqueryInFilter() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | where exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void testNotExistsSubquery() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where not exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
    verifyDataRowsInOrder(result, rows(1001, "Hello", 70000), rows(1004, "David", 0));
  }

  @Test
  public void testNotExistsSubqueryInFilter() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | where not exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
    verifyDataRowsInOrder(result, rows(1001, "Hello", 70000), rows(1004, "David", 0));
  }

  @Test
  public void testEmptyExistsSubquery() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where uid = 0000 AND id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
    verifyNumOfRows(result, 0);
  }

  @Test
  public void testUncorrelatedExistsSubquery() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where name = 'Tom'"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
    verifyNumOfRows(result, 7);

    result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where not exists ["
                    + "    source = %s | where name = 'Tom'"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 0);
  }

  @Test
  public void testUncorrelatedExistsSubqueryCheckTheReturnContentOfInnerTableIsEmptyOrNot()
      throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s"
                    + "  ]"
                    + "| eval constant = \\\"Bala\\\""
                    + "| fields constant",
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
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where uid = 999"
                    + "  ]"
                    + "| eval constant = 'Bala'"
                    + "| fields constant",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 0);
  }

  @Test
  public void testNestedExistsSubquery() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s"
                    + "    | where exists ["
                    + "        source = %s"
                    + "        | where %s.occupation = %s.occupation"
                    + "      ]"
                    + "    | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER,
                TEST_INDEX_WORK_INFORMATION,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void testExistsSubqueryWithConjunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s"
                    + "    | where id = uid AND"
                    + "      %s.name = %s.name AND"
                    + "      %s.occupation = %s.occupation"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER,
                TEST_INDEX_WORK_INFORMATION,
                TEST_INDEX_WORKER,
                TEST_INDEX_WORK_INFORMATION,
                TEST_INDEX_WORKER,
                TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
    verifyDataRowsInOrder(result, rows(1003, "David", 120000), rows(1000, "Jake", 100000));
  }

  @Test
  public void testIssue3566() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| fields id, country"
                    + "| where exists ["
                    + "    source = %s"
                    + "    | where id = uid"
                    + "  ]"
                    + "| stats count() by country",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchemaInOrder(result, schema("count()", "bigint"), schema("country", "string"));
    verifyDataRows(result, rows(1, null), rows(1, "England"), rows(1, "USA"), rows(2, "Canada"));
  }

  @Test
  public void testSubsearchMaxOut1() throws IOException {
    setSubsearchMaxOut(1);
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 1);
    resetSubsearchMaxOut();
  }

  @Test
  public void testSubsearchMaxOut2() throws IOException {
    setSubsearchMaxOut(2);
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where id = uid and department = 'DATA'"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 2);
    resetSubsearchMaxOut();
  }

  @Test
  public void testSubsearchMaxOut3() throws IOException {
    setSubsearchMaxOut(2);
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s "
                    + "    | where id = uid "
                    + "    | eval dept = department "
                    + "    | where dept = 'DATA' "
                    + "    | sort - dept"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 1);
    resetSubsearchMaxOut();
  }

  @Test
  public void testSubsearchMaxOut4() throws IOException {
    setSubsearchMaxOut(2);
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s "
                    + "    | eval dept = department "
                    + "    | where dept = 'DATA' "
                    + "    | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 2);
    resetSubsearchMaxOut();
  }

  @Test
  public void testSubsearchMaxOutUncorrelated() throws IOException {
    setSubsearchMaxOut(1);
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | join type=left uid %s"
                    + "    | eval dept = department "
                    + "    | where dept = 'DATA' "
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 7);
    resetSubsearchMaxOut();
  }

  @Test
  public void testSubsearchMaxOutZero1() throws IOException {
    setSubsearchMaxOut(0);
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where name = 'Tom'"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 0);

    result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where not exists ["
                    + "    source = %s | where name = 'Tom'"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 7);
    resetSubsearchMaxOut();
  }

  @Test
  public void testSubsearchMaxOutZero2() throws IOException {
    setSubsearchMaxOut(0);
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 0);
    result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where not exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 7);
    resetSubsearchMaxOut();
  }

  @Test
  public void testSubsearchMaxOutUnlimited() throws IOException {
    setSubsearchMaxOut(-1);
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where id = uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifyNumOfRows(result, 5);
    resetSubsearchMaxOut();
  }
}

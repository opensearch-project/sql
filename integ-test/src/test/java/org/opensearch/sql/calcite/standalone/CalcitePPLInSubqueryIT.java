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
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.exception.SemanticCheckException;

@Ignore
public class CalcitePPLInSubqueryIT extends CalcitePPLIntegTestCase {

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
  public void testSelfInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                " source = %s | where id in [ source = %s| where country = 'USA'| fields id]",
                TEST_INDEX_WORKER, TEST_INDEX_WORKER));
    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("id", "integer"),
        schema("salary", "integer"));
    verifyDataRows(
        result,
        rows("Hello", "USA", "Artist", 1001, 70000),
        rows("Tommy", "USA", "Teacher", 1006, 30000));
  }

  @Test
  public void testWhereInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | where id in [source = %s | fields uid ] | sort  - salary | fields id, name, salary",
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
  public void testFilterInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s id in [source = %s | fields uid]| sort  - salary| fields id, name, salary",
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
  public void testInSubqueryWithParentheses() {
    JSONObject result1 =
        executeQuery(
            String.format(
                "source = %s| where (id) in [source = %s | fields uid]| sort  - salary| fields id, name",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    JSONObject result2 =
        executeQuery(
            String.format(
                "source = %s (id) in [ source = %s | fields uid] | sort  - salary | fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result1, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifySchema(
        result2, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result1,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1006, "Tommy", 30000));
    verifyDataRowsInOrder(
        result2,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void testTwoExpressionsInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                " source = %s | where (id, name) in [ source = %s | fields uid, name] | sort  - salary | fields id, name, salary ",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000));
  }

  @Test
  public void testWhereNotInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | where id not in [ source = %s | fields uid] | sort  - salary | fields id, name, salary ",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(result, rows(1001, "Hello", 70000), rows(1004, "David", 0));
  }

  @Test
  public void testFilterNotInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s id not in [ source = %s | fields uid] | sort  - salary | fields id, name, salary ",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(result, rows(1001, "Hello", 70000), rows(1004, "David", 0));
  }

  @Test
  public void testTwoExpressionsNotInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | where (id, name) not in [ source = %s | fields uid, name] | sort  - salary | fields id, name, salary ",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result, rows(1001, "Hello", 70000), rows(1006, "Tommy", 30000), rows(1004, "David", 0));
  }

  @Test
  public void testEmptyInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s id not in [source = %s | where uid = 0000 | fields uid]| sort  - salary| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1000, "Jake", 100000),
        rows(1005, "Jane", 90000),
        rows(1001, "Hello", 70000),
        rows(1006, "Tommy", 30000),
        rows(1004, "David", 0));
  }

  @Test
  public void testNestedInSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s| where id in [source = %s| where occupation in [source = %s| where occupation != 'Engineer'| fields occupation ]| fields uid]| sort  - salary| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_OCCUPATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result,
        rows(1002, "John", 120000),
        rows(1003, "David", 120000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void testNestedInSubquery2() {
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s\n" +
                    "| where id in [\n" +
                    "    source = %s\n" +
                    "    | where occupation in [\n" +
                    "        source = %s\n" +
                    "        | where occupation != 'Engineer'\n" +
                    "        | fields occupation\n" +
                    "      ]\n" +
                    "    | fields uid\n" +
                    "  ]\n" +
                    "| sort  - salary\n",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_OCCUPATION));
    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("id", "integer"),
        schema("salary", "integer"));
    verifyDataRowsInOrder(
        result,
        rows("John", "Canada", "Doctor", 1002, 120000),
        rows("David", null, "Doctor", 1003, 120000),
        rows("Tommy", "USA", "Teacher", 1006, 30000));
  }

  @Ignore // TODO bug? fail in execution, the plan converted is correct
  public void testInSubqueryAsJoinFilter() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s| inner join left=a, right=b ON a.id = b.uid AND b.occupation in [source = %s | where occupation != 'Engineer' | fields occupation] %s | fields a.id, a.name, a.salary, b.occupation",
                TEST_INDEX_WORKER, TEST_INDEX_OCCUPATION, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(
        result,
        rows(1003, "David", 120000),
        rows(1002, "John", 120000),
        rows(1006, "Tommy", 30000));
  }

  @Test
  public void failWhenNumOfColumnsNotMatchOutputOfSubquery() {
    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s| where id in [source = %s | fields uid, department ]| sort  - salary | fields id, name, salary",
                        TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION)));
    verifyErrorMessageContains(
        e1,
        "The number of columns in the left hand side of an IN subquery does not match the number of"
            + " columns in the output of subquery");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s| where (id, name, salary) in [source = %s | fields uid, department] | sort  - salary| fields id, name, salary",
                        TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION)));
    verifyErrorMessageContains(
        e2,
        "The number of columns in the left hand side of an IN subquery does not match the number of"
            + " columns in the output of subquery");
  }

  @Test
  public void testInSubqueryWithTableAlias() {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s as o | where id in [ source = %s as i | where i.department = 'DATA' | fields uid] | sort - o.salary | fields o.id, o.name, o.salary ",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("salary", "integer"));
    verifyDataRowsInOrder(result, rows(1002, "John", 120000), rows(1005, "Jane", 90000));
  }
}

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
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;

public class CalcitePPLScalarSubqueryIT extends CalcitePPLIntegTestCase {

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
  public void testUncorrelatedScalarSubqueryInSelect() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | eval count_dept = [
                       source = %s | stats count(department)
                     ]
                   | fields name, count_dept
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("name", "string"), schema("count_dept", "long"));
    verifyDataRows(
        result,
        rows("Jake", 5),
        rows("Hello", 5),
        rows("John", 5),
        rows("David", 5),
        rows("David", 5),
        rows("Jane", 5),
        rows("Tommy", 5));
  }

  @Test
  public void testUncorrelatedScalarSubqueryInExpressionInSelect() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | eval count_dept = [
                       source = %s | stats count(department)
                     ] + 10
                   | fields name, count_dept
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("name", "string"), schema("count_dept", "long"));
    verifyDataRows(
        result,
        rows("Jake", 15),
        rows("Hello", 15),
        rows("John", 15),
        rows("David", 15),
        rows("David", 15),
        rows("Jane", 15),
        rows("Tommy", 15));
  }

  @Test
  public void testUncorrelatedScalarSubqueryInSelectAndWhere() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where id > [
                       source = %s | stats count(department)
                     ] + 999
                   | eval count_dept = [
                       source = %s | stats count(department)
                     ]
                   | fields name, count_dept
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("name", "string"), schema("count_dept", "long"));
    verifyDataRows(result, rows("Jane", 5), rows("Tommy", 5));
  }

  @Test
  public void testUncorrelatedScalarSubqueryInSelectAndInFilter() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s id > [ source = %s | stats count(department) ] + 999
                   | eval count_dept = [
                       source = %s | stats count(department)
                     ]
                   | fields name, count_dept
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("name", "string"), schema("count_dept", "long"));
    verifyDataRows(result, rows("Jane", 5), rows("Tommy", 5));
  }

  @Test
  public void testCorrelatedScalarSubqueryInSelect() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | eval count_dept = [
                       source = %s
                       | where id = uid | stats count(department)
                     ]
                   | fields id, name, count_dept
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("count_dept", "long"));
    verifyDataRows(
        result,
        rows(1000, "Jake", 1),
        rows(1001, "Hello", 0),
        rows(1002, "John", 1),
        rows(1003, "David", 1),
        rows(1004, "David", 0),
        rows(1005, "Jane", 1),
        rows(1006, "Tommy", 1));
  }

  @Test
  public void testCorrelatedScalarSubqueryInSelectWithNonEqual() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | eval count_dept = [
                       source = %s
                       | where id > uid | stats count(department)
                     ]
                   | fields id, name, count_dept
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(
        result, schema("id", "integer"), schema("name", "string"), schema("count_dept", "long"));
    verifyDataRows(
        result,
        rows(1000, "Jake", 0),
        rows(1001, "Hello", 1),
        rows(1002, "John", 1),
        rows(1003, "David", 2),
        rows(1004, "David", 3),
        rows(1005, "Jane", 3),
        rows(1006, "Tommy", 4));
  }

  @Test
  public void testCorrelatedScalarSubqueryInWhere() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where id = [
                       source = %s | where id = uid | stats max(uid)
                     ]
                   | fields id, name
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake"),
        rows(1002, "John"),
        rows(1003, "David"),
        rows(1005, "Jane"),
        rows(1006, "Tommy"));
  }

  @Test
  public void testCorrelatedScalarSubqueryInFilter() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s id = [ source = %s | where id = uid | stats max(uid) ]
                   | fields id, name
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake"),
        rows(1002, "John"),
        rows(1003, "David"),
        rows(1005, "Jane"),
        rows(1006, "Tommy"));
  }

  @Test
  public void testDisjunctiveCorrelatedScalarSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where [
                       source = %s | where id = uid OR uid = 1010 | stats count()
                     ] > 0
                   | fields id, name
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake"),
        rows(1002, "John"),
        rows(1003, "David"),
        rows(1005, "Jane"),
        rows(1006, "Tommy"));
  }

  @Test
  public void testTwoUncorrelatedScalarSubqueriesInOr() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where id = [
                       source = %s | sort uid | stats max(uid)
                     ] OR id = [
                       source = %s | sort uid | where department = 'DATA' | stats min(uid)
                     ]
                   | fields id, name
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(result, rows(1002, "John"), rows(1006, "Tommy"));
  }

  @Test
  public void testTwoCorrelatedScalarSubqueriesInOr() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where id = [
                       source = %s | where id = uid | stats max(uid)
                     ] OR id = [
                       source = %s | sort uid | where department = 'DATA' | stats min(uid)
                     ]
                   | fields id, name
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_WORK_INFORMATION));
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(
        result,
        rows(1000, "Jake"),
        rows(1002, "John"),
        rows(1003, "David"),
        rows(1005, "Jane"),
        rows(1006, "Tommy"));
  }

  @Test
  public void testNestedScalarSubquery() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                    source = %s
                    | where id = [
                        source = %s
                        | where uid = [
                            source = %s
                            | stats min(salary)
                          ] + 1000
                        | sort department
                        | stats max(uid)
                      ]
                    | fields id, name
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_OCCUPATION));
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(result, rows(1000, "Jake"));
  }

  @Test
  public void testNestedScalarSubqueryWithTableAlias() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s as o
                   | where id = [
                       source = %s as i
                       | where uid = [
                           source = %s as n
                           | stats min(n.salary)
                         ] + 1000
                       | sort i.department
                       | stats max(i.uid)
                     ]
                   | fields o.id, o.name
                   """,
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_OCCUPATION));
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(result, rows(1000, "Jake"));
  }
}

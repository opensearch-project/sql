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
                "source = " + TEST_INDEX_WORKER + "\n| eval count_dept = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | stats count(department)\n  ]\n| fields name, count_dept\n");
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
                "source = " + TEST_INDEX_WORKER + "\n| eval count_dept = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | stats count(department)\n  ] + 10\n| fields name, count_dept\n");
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
                "source = " + TEST_INDEX_WORKER + "\n| where id > [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | stats count(department)\n  ] + 999\n| eval count_dept = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | stats count(department)\n  ]\n| fields name, count_dept\n");
    verifySchema(result, schema("name", "string"), schema("count_dept", "long"));
    verifyDataRows(result, rows("Jane", 5), rows("Tommy", 5));
  }

  @Test
  public void testUncorrelatedScalarSubqueryInSelectAndInFilter() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_WORKER + " id > [ source = " + TEST_INDEX_WORK_INFORMATION + " | stats count(department) ] + 999\n| eval count_dept = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | stats count(department)\n  ]\n| fields name, count_dept\n");
    verifySchema(result, schema("name", "string"), schema("count_dept", "long"));
    verifyDataRows(result, rows("Jane", 5), rows("Tommy", 5));
  }

  @Test
  public void testCorrelatedScalarSubqueryInSelect() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_WORKER + "\n| eval count_dept = [\n    source = " + TEST_INDEX_WORK_INFORMATION + "\n    | where id = uid | stats count(department)\n  ]\n| fields id, name, count_dept\n");
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
                "source = " + TEST_INDEX_WORKER + "\n| eval count_dept = [\n    source = " + TEST_INDEX_WORK_INFORMATION + "\n    | where id > uid | stats count(department)\n  ]\n| fields id, name, count_dept\n");
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
                "source = " + TEST_INDEX_WORKER + "\n| where id = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | where id = uid | stats max(uid)\n  ]\n| fields id, name\n");
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
                "source = " + TEST_INDEX_WORKER + " id = [ source = " + TEST_INDEX_WORK_INFORMATION + " | where id = uid | stats max(uid) ]\n| fields id, name\n");
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
                "source = " + TEST_INDEX_WORKER + "\n| where [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | where id = uid OR uid = 1010 | stats count()\n  ] > 0\n| fields id, name\n");
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
                "source = " + TEST_INDEX_WORKER + "\n| where id = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | sort uid | stats max(uid)\n  ] OR id = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | sort uid | where department = 'DATA' | stats min(uid)\n  ]\n| fields id, name\n");
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(result, rows(1002, "John"), rows(1006, "Tommy"));
  }

  @Test
  public void testTwoCorrelatedScalarSubqueriesInOr() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_WORKER + "\n| where id = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | where id = uid | stats max(uid)\n  ] OR id = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " | sort uid | where department = 'DATA' | stats min(uid)\n  ]\n| fields id, name\n");
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
                " source = " + TEST_INDEX_WORKER + "\n | where id = [\n     source = " + TEST_INDEX_WORK_INFORMATION + "\n     | where uid = [\n         source = " + TEST_INDEX_OCCUPATION + "\n         | stats min(salary)\n       ] + 1000\n     | sort department\n     | stats max(uid)\n   ]\n | fields id, name\n");
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(result, rows(1000, "Jake"));
  }

  @Test
  public void testNestedScalarSubqueryWithTableAlias() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_WORKER + " as o\n| where id = [\n    source = " + TEST_INDEX_WORK_INFORMATION + " as i\n    | where uid = [\n        source = " + TEST_INDEX_OCCUPATION + " as n\n        | stats min(n.salary)\n      ] + 1000\n    | sort i.department\n    | stats max(i.uid)\n  ]\n| fields o.id, o.name\n");
    verifySchema(result, schema("id", "integer"), schema("name", "string"));
    verifyDataRows(result, rows(1000, "Jake"));
  }
}

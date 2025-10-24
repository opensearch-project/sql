/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalciteDynamicFieldsCommandIT extends CalcitePPLPermissiveIntegTestCase {

  private static final String TEST_INDEX_DYNAMIC = "test_dynamic_fields";

  @Override
  public void init() throws IOException {
    super.init();
    createTestIndexWithUnmappedFields();
    enableCalcite();
  }

  @Test
  public void testBasicProjection() throws IOException {
    String query =
        source(TEST_INDEX_DYNAMIC, "fields firstname, lastname, department, salary | head 1");
    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(fetch=[1])\n"
            + "        LogicalProject(firstname=[$0], lastname=[$2], department=[ITEM($9,"
            + " 'department')], salary=[ITEM($9, 'salary')])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=['department'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=['salary'], expr#13=[ITEM($t9, $t12)],"
            + " firstname=[$t0], lastname=[$t2], department=[$t11], salary=[$t13])\n"
            + "        EnumerableLimit(fetch=[1])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("department", "string"),
        schema("salary", "int"));
    verifyDataRows(result, rows("John", "Doe", "Engineering", 75000));
  }

  @Test
  public void testEval() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eval salary = cast(salary as int) * 2 | fields firstname,"
                + " lastname, salary | head 1");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(fetch=[1])\n"
            + "        LogicalProject(firstname=[$0], lastname=[$2], salary=[*(SAFE_CAST(ITEM($9,"
            + " 'salary')), 2)])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=['salary'], expr#11=[ITEM($t9,"
            + " $t10)], expr#12=[SAFE_CAST($t11)], expr#13=[2], expr#14=[*($t12, $t13)],"
            + " firstname=[$t0], lastname=[$t2], salary=[$t14])\n"
            + "        EnumerableLimit(fetch=[1])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("salary", "int"));
  }

  private void createTestIndexWithUnmappedFields() throws IOException {
    if (isIndexExist(client(), TEST_INDEX_DYNAMIC)) {
      return;
    }

    String mapping =
        "{"
            + "\"mappings\": {"
            // Disable dynamic mapping - extra fields won't be indexed but will be stored
            + "  \"dynamic\": false,"
            + "  \"properties\": {"
            + "    \"firstname\": {\"type\": \"text\"},"
            + "    \"lastname\": {\"type\": \"text\"},"
            + "    \"accountnumber\": {\"type\": \"long\"}"
            + "  }"
            + "}"
            + "}";

    createIndexByRestClient(client(), TEST_INDEX_DYNAMIC, mapping);

    String bulkData =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"account_number\":1,\"city\":\"NYC\",\"department\":\"Engineering\",\"salary\":75000}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"firstname\":\"Jane\",\"lastname\":\"Smith\",\"account_number\":2,\"city\":\"LA\",\"department\":\"Marketing\",\"salary\":65000}\n"
            + "{\"index\":{\"_id\":\"3\"}}\n"
            + "{\"firstname\":\"Bob\",\"lastname\":\"Johnson\",\"account_number\":3,\"city\":\"Chicago\",\"department\":\"Sales\",\"salary\":55000}\n"
            + "{\"index\":{\"_id\":\"4\"}}\n"
            + "{\"firstname\":\"Alice\",\"lastname\":\"Brown\",\"account_number\":4,\"city\":\"Seattle\",\"department\":\"Engineering\",\"salary\":80000}\n"
            + "{\"index\":{\"_id\":\"5\"}}\n"
            + "{\"firstname\":\"Charlie\",\"lastname\":\"Wilson\",\"account_number\":5,\"city\":\"Boston\",\"department\":\"HR\",\"salary\":60000}\n";

    Request request = new Request("POST", "/" + TEST_INDEX_DYNAMIC + "/_bulk?refresh=true");
    request.setJsonEntity(bulkData);
    client().performRequest(request);
  }
}

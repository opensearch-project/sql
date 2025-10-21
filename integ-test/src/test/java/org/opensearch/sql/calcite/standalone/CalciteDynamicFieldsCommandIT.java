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
    JSONObject result =
        executeQuery(
            source(TEST_INDEX_DYNAMIC, "fields firstname, lastname, department, salary | head 1"));

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
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC,
                "eval salary = cast(salary as int) * 2 | fields firstname,"
                    + " lastname, salary | head 1"));

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
            + "  \"dynamic\": false," // Disable dynamic mapping - extra fields won't be indexed but
            // will be stored
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

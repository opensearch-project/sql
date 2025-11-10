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
import org.opensearch.sql.common.utils.JsonUtils;

public class CalciteDynamicFieldsTimechartIT extends CalcitePPLPermissiveIntegTestCase {

  private static final String TEST_INDEX_DYNAMIC = "test_dynamic_fields";

  @Override
  public void init() throws IOException {
    super.init();
    createTestIndexWithUnmappedFields();
    enableCalcite();
  }

  @Test
  public void testTimechartByDynamicField() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "timechart span=1d count() by event");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("event", "string"),
        schema("count", "bigint"));
    verifyDataRows(
        result,
        rows("2025-10-25 00:00:00", "get", 3),
        rows("2025-10-25 00:00:00", "post", 1),
        rows("2025-10-25 00:00:00", "put", 1));
  }

  @Test
  public void testTimechartWithDynamicField() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "timechart span=1d avg(latency)");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("@timestamp", "string"), schema("avg(latency)", "double"));
    verifyDataRows(result, rows("2025-10-25 00:00:00", 62));
  }

  @Test
  public void testTrendlineWithDynamicFieldWithCast() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "trendline sma(2, latency) as latency_trend| fields id, latency, latency_trend");
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("latency", "int"),
        schema("latency_trend", "double"));
    verifyDataRows(
        result,
        rows(1, 10, null),
        rows(2, 20, 15),
        rows(3, 40, 30),
        rows(4, 80, 60),
        rows(5, 160, 120));
  }

  @Test
  public void testTrendlineSortByDynamicField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "trendline sort event sma(2, latency) as latency_trend "
                + "| fields id, latency, latency_trend");
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("latency", "int"),
        schema("latency_trend", "double"));
    verifyDataRows(
        result,
        rows(1, 10, null),
        rows(3, 40, 25),
        rows(5, 160, 100),
        rows(2, 20, 90),
        rows(4, 80, 50));
  }

  private void createTestIndexWithUnmappedFields() throws IOException {
    if (isIndexExist(client(), TEST_INDEX_DYNAMIC)) {
      return;
    }

    String mapping =
        JsonUtils.sjson(
            "{",
            "'mappings': {",
            // Disable dynamic mapping - extra fields won't be indexed but will be stored
            "  'dynamic': false,",
            "  'properties': {",
            "    'id': {'type': 'long'},",
            "    'name': {'type': 'text'}",
            "  }",
            "}",
            "}");

    createIndexByRestClient(client(), TEST_INDEX_DYNAMIC, mapping);

    String bulkData =
        JsonUtils.sjson(
            "{'index': {'_id':'1'}}",
            "{'id':1,'event':'get','latency':10,'@timestamp':'2025-10-25T11:12:13.123Z'}",
            "{'index':{'_id':'2'}}",
            "{'id':2,'event':'post','latency':20,'@timestamp':'2025-10-25T12:12:13.123Z'}",
            "{'index':{'_id':'3'}}",
            "{'id':3,'event':'get','latency':40,'@timestamp':'2025-10-25T13:12:13.123Z'}",
            "{'index':{'_id':'4'}}",
            "{'id':4,'event':'put','latency':80,'@timestamp':'2025-10-25T14:12:13.123Z'}",
            "{'index':{'_id':'5'}}",
            "{'id':5,'event':'get','latency':160,'@timestamp':'2025-10-25T15:12:13.123Z'}");

    Request request = new Request("POST", "/" + TEST_INDEX_DYNAMIC + "/_bulk?refresh=true");
    request.setJsonEntity(bulkData);
    client().performRequest(request);
  }
}

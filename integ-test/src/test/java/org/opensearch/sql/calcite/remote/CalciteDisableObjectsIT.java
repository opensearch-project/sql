/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assume.assumeFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteDisableObjectsIT extends PPLIntegTestCase {

  private static final String TEST_INDEX = "test_disable_objects";
  private static final String TEST_PATH = "/var/log/app.log";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    if (isAnalyticsParquetIndicesEnabled() || TestUtils.isIndexExist(client(), TEST_INDEX)) {
      return;
    }

    TestUtils.createIndexByRestClient(
        client(),
        TEST_INDEX,
        "{\"mappings\":{\"properties\":{\"attributes\":"
            + "{\"type\":\"object\",\"disable_objects\":true}}}}}");
    Request document = new Request("PUT", "/" + TEST_INDEX + "/_doc/1?refresh=true");
    document.setJsonEntity("{\"attributes\":{\"log.file.path\":\"" + TEST_PATH + "\"}}");
    client().performRequest(document);
  }

  @Test
  public void testSourceDoesNotReturnFlattenedFieldsAlongsideStructParent() throws IOException {
    assumeFalse(
        "disable_objects is an OpenSearch mapping feature", isAnalyticsParquetIndicesEnabled());

    JSONObject result = executeQuery("source=" + TEST_INDEX);

    verifySchema(result, schema("attributes", "struct"));
    JSONArray row = result.getJSONArray("datarows").getJSONArray(0);
    assertEquals(1, row.length());
    assertEquals(
        TEST_PATH,
        row.getJSONObject(0).getJSONObject("log").getJSONObject("file").getString("path"));
  }
}

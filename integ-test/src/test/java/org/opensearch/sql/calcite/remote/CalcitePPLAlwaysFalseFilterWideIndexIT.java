/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.Capability.PROFILE_SHAPE;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

/** Planning an always-false filter must not grow with the field count (#5952). */
@RequiresCapability(PROFILE_SHAPE)
public class CalcitePPLAlwaysFalseFilterWideIndexIT extends PPLIntegTestCase {

  private static final String INDEX = "otel_logs_wide";
  private static final int ATTRIBUTE_COUNT = 2000;
  private static final double MAX_PLANNING_MS = 1000;

  // All fields top-level, so every field stays in the output of the empty result. Without the fix,
  // the Volcano planner's empty pruning and EnumerableValues conversion took seconds here.
  private static final String FLAT_INDEX = "wide_flat";
  private static final int FLAT_FIELD_COUNT = 4000;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    if (!isIndexExist(client(), INDEX)) {
      createWideIndex();
    }
    if (!isIndexExist(client(), FLAT_INDEX)) {
      createFlatWideIndex();
    }
  }

  @Test
  public void literalComparedToLiteralOnFlatWideIndex() throws IOException {
    assertFastAndEmpty("source=" + FLAT_INDEX + " | where 'a' = 'b' | head 10");
  }

  @Test
  public void filtersThatContradictOnlyAfterMergeOnFlatWideIndex() throws IOException {
    assertFastAndEmpty(
        "source=" + FLAT_INDEX + " | where f0 = 'a' | where f0 = 'b' | eval x = 1 | head 10");
  }

  @Test
  public void literalComparedToLiteral() throws IOException {
    assertFastAndEmpty("source=" + INDEX + " | where 'a' = 'b' | eval x = 1 | head 10");
  }

  @Test
  public void numericFieldNotEqualEmptyString() throws IOException {
    assertFastAndEmpty(
        "source="
            + INDEX
            + " | where isnotnull(severityNumber) and severityNumber != '' | eval x = 1 | head 10");
  }

  @Test
  public void filtersThatContradictOnlyAfterMerge() throws IOException {
    assertFastAndEmpty(
        "source=" + INDEX + " | where severityText = 'a' | where severityText = 'b' | head 10");
  }

  @Test
  public void alwaysFalseFilterUnderEvalSortAndFields() throws IOException {
    assertFastAndEmpty(
        "source="
            + INDEX
            + " | where 'a' = 'b' | eval x = 1, y = 2 | sort severityText"
            + " | fields severityText, x, y");
  }

  private void assertFastAndEmpty(String query) throws IOException {
    profile(query); // warm-up
    JSONObject response = profile(query);
    assertEquals(0, response.getJSONArray("datarows").length());
    JSONObject phases = response.getJSONObject("profile").getJSONObject("phases");
    double planningMs =
        phases.getJSONObject("analyze").getDouble("time_ms")
            + phases.getJSONObject("optimize").getDouble("time_ms");
    assertTrue(
        "analyze + optimize took " + planningMs + " ms: " + phases, planningMs < MAX_PLANNING_MS);
  }

  private JSONObject profile(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(new JSONObject().put("query", query).put("profile", true).toString());
    return new JSONObject(getResponseBody(client().performRequest(request), true));
  }

  /** An OpenTelemetry logs index: 16 top-level fields plus 2,000 keyword attributes. */
  private void createWideIndex() throws IOException {
    JSONObject attributes = new JSONObject();
    for (int i = 0; i < ATTRIBUTE_COUNT; i++) {
      attributes.put("attr_" + i, type("keyword"));
    }
    JSONObject properties =
        new JSONObject()
            .put("@timestamp", type("date"))
            .put("observedTimestamp", type("date"))
            .put("time", type("date"))
            .put("severityText", type("keyword"))
            .put("severityNumber", type("long"))
            .put("body", type("text"))
            .put("traceId", type("keyword"))
            .put("spanId", type("keyword"))
            .put("flags", type("long"))
            .put("droppedAttributesCount", type("long"))
            .put("serviceName", type("keyword"))
            .put("schemaUrl", type("keyword"))
            .put("instrumentationScopeName", type("keyword"))
            .put("instrumentationScopeVersion", type("keyword"))
            .put("eventName", type("keyword"))
            .put("hostName", type("keyword"))
            .put("attributes", new JSONObject().put("properties", attributes));
    JSONObject body =
        new JSONObject()
            .put("settings", new JSONObject().put("index.mapping.total_fields.limit", 5000))
            .put("mappings", new JSONObject().put("properties", properties));
    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(body.toString());
    client().performRequest(create);

    Request doc = new Request("PUT", "/" + INDEX + "/_doc/1?refresh=true");
    doc.setJsonEntity(
        new JSONObject()
            .put("@timestamp", "2026-01-01T00:00:00Z")
            .put("severityText", "INFO")
            .put("severityNumber", 9)
            .put("body", "hello")
            .put("attributes", new JSONObject().put("attr_0", "v"))
            .toString());
    client().performRequest(doc);
  }

  /** An index with {@value #FLAT_FIELD_COUNT} top-level keyword fields and one document. */
  private void createFlatWideIndex() throws IOException {
    JSONObject properties = new JSONObject();
    for (int i = 0; i < FLAT_FIELD_COUNT; i++) {
      properties.put("f" + i, type("keyword"));
    }
    JSONObject body =
        new JSONObject()
            .put(
                "settings",
                new JSONObject().put("index.mapping.total_fields.limit", FLAT_FIELD_COUNT + 100))
            .put("mappings", new JSONObject().put("properties", properties));
    Request create = new Request("PUT", "/" + FLAT_INDEX);
    create.setJsonEntity(body.toString());
    client().performRequest(create);

    Request doc = new Request("PUT", "/" + FLAT_INDEX + "/_doc/1?refresh=true");
    doc.setJsonEntity(new JSONObject().put("f0", "v").toString());
    client().performRequest(doc);
  }

  private static JSONObject type(String type) {
    return new JSONObject().put("type", type);
  }
}

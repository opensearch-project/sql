/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.sql.legacy.TestUtils.AnalyticsIndexConfig;

/**
 * Pure-logic coverage for the analytics-engine field strip (no filesystem / cluster). Verifies that
 * nested/geo_point/geo_shape/binary/alias fields are removed from mappings and bulk data on the AE
 * route, and that everything is a no-op when the route is off.
 */
public class AnalyticsFieldStripTests {

  @After
  public void clearFlag() {
    System.clearProperty(AnalyticsIndexConfig.ENABLED_PROP);
  }

  private void enable() {
    System.setProperty(AnalyticsIndexConfig.ENABLED_PROP, "true");
  }

  private static final String MAPPING =
      "{\"mappings\":{\"properties\":{"
          + "\"keep_text\":{\"type\":\"text\"},"
          + "\"nested_value\":{\"type\":\"nested\"},"
          + "\"geo_point_value\":{\"type\":\"geo_point\"},"
          + "\"geo_shape_value\":{\"type\":\"geo_shape\"},"
          + "\"binary_value\":{\"type\":\"binary\"},"
          + "\"alias_value\":{\"type\":\"alias\",\"path\":\"keep_text\"},"
          + "\"obj\":{\"properties\":{"
          + "  \"keep_inner\":{\"type\":\"keyword\"},"
          + "  \"inner_geo\":{\"type\":\"geo_point\"}}}"
          + "}}}";

  @Test
  public void mappingStrip_removesUnsupportedRecursively_andReportsTopLevel() {
    enable();
    JSONObject json = new JSONObject(MAPPING);
    Set<String> dropped = AnalyticsIndexConfig.stripUnsupportedMappingFields(json);

    assertEquals(
        Set.of("nested_value", "geo_point_value", "geo_shape_value", "binary_value", "alias_value"),
        dropped);

    JSONObject props = json.getJSONObject("mappings").getJSONObject("properties");
    assertTrue("supported scalar kept", props.has("keep_text"));
    assertFalse(props.has("nested_value"));
    assertFalse(props.has("geo_point_value"));
    assertFalse(props.has("geo_shape_value"));
    assertFalse(props.has("binary_value"));
    assertFalse(props.has("alias_value"));

    // object field kept, but its unsupported sub-property stripped recursively
    assertTrue(props.has("obj"));
    JSONObject inner = props.getJSONObject("obj").getJSONObject("properties");
    assertTrue(inner.has("keep_inner"));
    assertFalse("nested geo_point inside object dropped", inner.has("inner_geo"));
  }

  @Test
  public void mappingStrip_noopWhenDisabled() {
    JSONObject json = new JSONObject(MAPPING);
    Set<String> dropped = AnalyticsIndexConfig.stripUnsupportedMappingFields(json);
    assertTrue(dropped.isEmpty());
    assertTrue(json.getJSONObject("mappings").getJSONObject("properties").has("nested_value"));
  }

  @Test
  public void bulkStrip_removesDroppedKeysFromSourceLinesOnly() {
    enable();
    String bulk =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"keep_text\":\"x\",\"geo_point_value\":{\"lat\":1,\"lon\":2},\"binary_value\":\"AA==\"}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"keep_text\":\"y\",\"nested_value\":[{\"a\":1}]}\n";
    String out =
        AnalyticsIndexConfig.stripBulkFields(
            bulk, Set.of("geo_point_value", "binary_value", "nested_value"));

    String[] lines = out.split("\n");
    // action lines untouched
    assertTrue(lines[0].contains("\"index\""));
    assertTrue(lines[2].contains("\"index\""));
    // source lines stripped, supported field retained
    JSONObject doc1 = new JSONObject(lines[1]);
    assertTrue(doc1.has("keep_text"));
    assertFalse(doc1.has("geo_point_value"));
    assertFalse(doc1.has("binary_value"));
    JSONObject doc2 = new JSONObject(lines[3]);
    assertTrue(doc2.has("keep_text"));
    assertFalse(doc2.has("nested_value"));
  }

  @Test
  public void bulkStrip_noopWhenDisabledOrEmptyDropSet() {
    String bulk = "{\"index\":{}}\n{\"geo_point_value\":{\"lat\":1}}\n";
    // disabled -> unchanged even with a drop set
    assertEquals(bulk, AnalyticsIndexConfig.stripBulkFields(bulk, Set.of("geo_point_value")));
    // enabled but empty drop set -> unchanged
    enable();
    assertEquals(bulk, AnalyticsIndexConfig.stripBulkFields(bulk, Set.of()));
  }
}

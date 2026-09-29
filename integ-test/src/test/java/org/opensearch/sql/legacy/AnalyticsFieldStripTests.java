/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.sql.legacy.TestUtils.AnalyticsIndexConfig;

/**
 * Pure-logic coverage for the analytics-engine field strip (no filesystem / cluster). Verifies that
 * nested/geo_point/geo_shape/alias fields are removed from mappings and bulk data on the AE route —
 * including <em>nested</em> paths and arrays of objects — that a supported {@code binary} field is
 * left intact, and that everything is a no-op when the route is off.
 */
public class AnalyticsFieldStripTests {

  @After
  public void clearFlag() {
    System.clearProperty(AnalyticsIndexConfig.ENABLED_PROP);
  }

  private void enable() {
    System.setProperty(AnalyticsIndexConfig.ENABLED_PROP, "true");
  }

  private static List<String> path(String... parts) {
    return List.of(parts);
  }

  private static final String MAPPING =
      "{\"mappings\":{\"properties\":{"
          + "\"keep_text\":{\"type\":\"text\"},"
          + "\"nested_value\":{\"type\":\"nested\"},"
          + "\"geo_point_value\":{\"type\":\"geo_point\"},"
          + "\"geo_shape_value\":{\"type\":\"geo_shape\"},"
          // default binary (store:false) — parquet store can't create it → stripped.
          + "\"binary_value\":{\"type\":\"binary\"},"
          // binary with store:true — engine reads it as VARBINARY → kept.
          + "\"binary_stored\":{\"type\":\"binary\",\"store\":true},"
          + "\"alias_value\":{\"type\":\"alias\",\"path\":\"keep_text\"},"
          + "\"obj\":{\"properties\":{"
          + "  \"keep_inner\":{\"type\":\"keyword\"},"
          + "  \"inner_geo\":{\"type\":\"geo_point\"}}}"
          + "}}}";

  @Test
  public void mappingStrip_removesUnsupportedRecursively_andReportsExactPaths() {
    enable();
    JSONObject json = new JSONObject(MAPPING);
    Set<List<String>> dropped = AnalyticsIndexConfig.stripUnsupportedMappingFields(json);

    assertEquals(
        Set.of(
            path("nested_value"),
            path("geo_point_value"),
            path("geo_shape_value"),
            // default binary (store:false) is dropped — parquet store can't create it.
            path("binary_value"),
            path("alias_value"),
            // recursive: the geo_point under the object reports its full path.
            path("obj", "inner_geo")),
        dropped);

    JSONObject props = json.getJSONObject("mappings").getJSONObject("properties");
    assertTrue("supported scalar kept", props.has("keep_text"));
    assertTrue("store:true binary is scannable — must be kept", props.has("binary_stored"));
    assertFalse(
        "store:false binary not creatable on parquet store — dropped", props.has("binary_value"));
    assertFalse(props.has("nested_value"));
    assertFalse(props.has("geo_point_value"));
    assertFalse(props.has("geo_shape_value"));
    assertFalse(props.has("alias_value"));

    // object field kept, but its unsupported sub-property stripped recursively
    assertTrue(props.has("obj"));
    JSONObject inner = props.getJSONObject("obj").getJSONObject("properties");
    assertTrue(inner.has("keep_inner"));
    assertFalse("nested geo_point inside object dropped", inner.has("inner_geo"));
  }

  /**
   * complex_geo-shaped coverage (the case the top-level-only strip used to miss): a {@code
   * geo_point} buried under object fields. Mapping must drop the exact nested path; bulk source
   * must drop the same nested value while keeping unaffected siblings.
   */
  @Test
  public void complexGeoShape_stripsNestedPath_andKeepsSiblings() {
    enable();
    String mapping =
        "{\"mappings\":{\"properties\":{"
            + "\"location\":{\"properties\":{"
            + "  \"point\":{\"type\":\"geo_point\"},"
            + "  \"name\":{\"type\":\"text\"},"
            + "  \"city\":{\"type\":\"keyword\"},"
            + "  \"country\":{\"type\":\"keyword\"}}},"
            + "\"nested_locations\":{\"properties\":{"
            + "  \"primary\":{\"properties\":{\"office\":{\"type\":\"geo_point\"}}}}}"
            + "}}}";
    JSONObject json = new JSONObject(mapping);
    Set<List<String>> dropped = AnalyticsIndexConfig.stripUnsupportedMappingFields(json);

    // exact paths, not top-level names
    assertEquals(
        Set.of(path("location", "point"), path("nested_locations", "primary", "office")), dropped);

    JSONObject loc = json.getJSONObject("mappings").getJSONObject("properties");
    JSONObject locProps = loc.getJSONObject("location").getJSONObject("properties");
    assertFalse("location.point removed", locProps.has("point"));
    assertTrue("location.name kept", locProps.has("name"));
    assertTrue("location.city kept", locProps.has("city"));
    assertTrue("location.country kept", locProps.has("country"));

    // now strip a matching source doc by those exact paths
    String bulk =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"location\":{\"point\":{\"lat\":1,\"lon\":2},\"name\":\"hq\",\"city\":\"sea\","
            + "\"country\":\"us\"},"
            + "\"nested_locations\":{\"primary\":{\"office\":{\"lat\":3,\"lon\":4}}}}\n";
    String out = AnalyticsIndexConfig.stripBulkFields(bulk, dropped);
    JSONObject doc = new JSONObject(out.split("\n")[1]);
    JSONObject docLoc = doc.getJSONObject("location");
    assertFalse("source location.point removed", docLoc.has("point"));
    assertTrue("source location.name kept", docLoc.has("name"));
    assertTrue("source location.city kept", docLoc.has("city"));
    assertTrue("source location.country kept", docLoc.has("country"));
    assertFalse(
        "source nested_locations.primary.office removed",
        doc.getJSONObject("nested_locations").getJSONObject("primary").has("office"));
  }

  /** A dropped path may sit under an array of objects — every element must be stripped. */
  @Test
  public void bulkStrip_handlesArraysOfObjects() {
    enable();
    String bulk =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"offices\":[{\"loc\":{\"lat\":1},\"name\":\"a\"},{\"loc\":{\"lat\":2},"
            + "\"name\":\"b\"}]}\n";
    String out = AnalyticsIndexConfig.stripBulkFields(bulk, Set.of(path("offices", "loc")));
    JSONObject doc = new JSONObject(out.split("\n")[1]);
    var arr = doc.getJSONArray("offices");
    for (int i = 0; i < arr.length(); i++) {
      JSONObject office = arr.getJSONObject(i);
      assertFalse("offices[" + i + "].loc removed", office.has("loc"));
      assertTrue("offices[" + i + "].name kept", office.has("name"));
    }
  }

  @Test
  public void mappingStrip_noopWhenDisabled() {
    JSONObject json = new JSONObject(MAPPING);
    Set<List<String>> dropped = AnalyticsIndexConfig.stripUnsupportedMappingFields(json);
    assertTrue(dropped.isEmpty());
    assertTrue(json.getJSONObject("mappings").getJSONObject("properties").has("nested_value"));
  }

  @Test
  public void bulkStrip_removesDroppedPathsFromSourceLinesOnly() {
    enable();
    String bulk =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"keep_text\":\"x\",\"geo_point_value\":{\"lat\":1,\"lon\":2},\"geo_shape_value\":\"POINT(1"
            + " 2)\"}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"keep_text\":\"y\",\"nested_value\":[{\"a\":1}]}\n";
    String out =
        AnalyticsIndexConfig.stripBulkFields(
            bulk, Set.of(path("geo_point_value"), path("geo_shape_value"), path("nested_value")));

    String[] lines = out.split("\n");
    // action lines untouched
    assertTrue(lines[0].contains("\"index\""));
    assertTrue(lines[2].contains("\"index\""));
    // source lines stripped, supported field retained
    JSONObject doc1 = new JSONObject(lines[1]);
    assertTrue(doc1.has("keep_text"));
    assertFalse(doc1.has("geo_point_value"));
    assertFalse(doc1.has("geo_shape_value"));
    JSONObject doc2 = new JSONObject(lines[3]);
    assertTrue(doc2.has("keep_text"));
    assertFalse(doc2.has("nested_value"));
  }

  @Test
  public void bulkStrip_leavesUntouchedSourceLinesByteForByte() {
    enable();
    // doc1 carries a dropped key (gets rewritten); doc2 does not (must pass through verbatim).
    String docWithDrop = "{\"keep_text\":\"x\",\"nested_value\":[{\"a\":1}]}";
    String docNoDrop = "{\"keep_text\":\"y\",\"age\":  30,\"z\":1}";
    String bulk =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + docWithDrop
            + "\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + docNoDrop
            + "\n";
    String out = AnalyticsIndexConfig.stripBulkFields(bulk, Set.of(path("nested_value")));
    String[] lines = out.split("\n", -1);
    // The doc that had no dropped key is byte-for-byte identical (odd spacing/key order preserved).
    assertEquals(docNoDrop, lines[3]);
    // The doc that had a dropped key lost it.
    assertFalse(new JSONObject(lines[1]).has("nested_value"));
  }

  @Test
  public void bulkStrip_noopWhenDisabledOrEmptyDropSet() {
    String bulk = "{\"index\":{}}\n{\"geo_point_value\":{\"lat\":1}}\n";
    // disabled -> unchanged even with a drop set
    assertEquals(bulk, AnalyticsIndexConfig.stripBulkFields(bulk, Set.of(path("geo_point_value"))));
    // enabled but empty drop set -> unchanged
    enable();
    assertEquals(bulk, AnalyticsIndexConfig.stripBulkFields(bulk, Set.of()));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_COMPLEX_GEO;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GEOPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.sql.GeopointFormatsIT;

public class GeoPointFormatsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.GEOPOINTS);
    loadIndex(Index.COMPLEX_GEO);
  }

  @Test
  public void testReadingGeopoints() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("search source=%s | head 5 | fields point ", TEST_INDEX_GEOPOINT));
    verifySchema(result, schema("point", null, "geo_point"));
    verifyDataRows(
        result,
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)));
  }

  @Test
  public void testReadingGeoHash() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where _id = '6' | fields point ", TEST_INDEX_GEOPOINT));
    verifySchema(result, schema("point", null, "geo_point"));
    Pair<Double, Double> point = GeopointFormatsIT.getGeoValue(result);
    assertEquals(40.71, point.getLeft(), GeopointFormatsIT.TOLERANCE);
    assertEquals(74, point.getRight(), GeopointFormatsIT.TOLERANCE);
  }

  // Complex geo tests - geo points within complex types (Maps)
  @Test
  public void testGeoPointInSimpleMap() throws IOException {
    String query =
        String.format(
            "search source=%s | where id = '1' | fields location", TEST_INDEX_COMPLEX_GEO);

    JSONObject result = executeQuery(query);
    verifySchema(result, schema("location", null, "struct"));

    // Verify the map contains the geo point properly converted
    // Using exact precision from complex_geo.json: {"lat": 47.6062, "lon": -122.3321}
    verifyDataRows(
        result,
        rows(
            Map.of(
                "name", "Seattle Office",
                "point", Map.of("lat", 47.6062, "lon", -122.3321),
                "city", "Seattle",
                "country", "USA")));
  }

  @Test
  public void testGeoPointInMapWithStringFormat() throws IOException {
    String query =
        String.format(
            "search source=%s | where id = '2' | fields location", TEST_INDEX_COMPLEX_GEO);

    JSONObject result = executeQuery(query);
    verifySchema(result, schema("location", null, "struct"));

    // Verify the map contains geo point parsed from string format
    // Using exact precision from complex_geo.json: "35.6762,139.6503"
    verifyDataRows(
        result,
        rows(
            Map.of(
                "name", "Tokyo Office",
                "point", Map.of("lat", 35.6762, "lon", 139.6503),
                "city", "Tokyo",
                "country", "Japan")));
  }

  @Test
  public void testNestedMapsWithGeoPoints() throws IOException {
    String query =
        String.format(
            "search source=%s | where id = '3' | fields nested_locations", TEST_INDEX_COMPLEX_GEO);

    JSONObject result = executeQuery(query);
    verifySchema(result, schema("nested_locations", null, "struct"));

    // Verify nested structure with multiple geo points
    // Using exact precision from complex_geo.json
    verifyDataRows(
        result,
        rows(
            Map.of(
                "primary",
                    Map.of(
                        "office", Map.of("lat", 37.7749, "lon", -122.4194),
                        "warehouse", Map.of("lat", 37.4419, "lon", -122.143)),
                "secondary",
                    Map.of(
                        "branch", Map.of("lat", 37.3382, "lon", -121.8863),
                        "store", Map.of("lat", 37.3688, "lon", -122.0363)))));
  }

  @Test
  public void testNestedMapsWithStringGeoPoints() throws IOException {
    String query =
        String.format(
            "search source=%s | where id = '4' | fields nested_locations", TEST_INDEX_COMPLEX_GEO);

    JSONObject result = executeQuery(query);
    verifySchema(result, schema("nested_locations", null, "struct"));

    // Verify nested structure with geo points in string format
    // Using exact precision from complex_geo.json: "40.7128,-74.0060" etc.
    verifyDataRows(
        result,
        rows(
            Map.of(
                "primary",
                    Map.of(
                        "office", Map.of("lat", 40.7128, "lon", -74.006),
                        "warehouse", Map.of("lat", 40.758, "lon", -73.9855)),
                "secondary",
                    Map.of(
                        "branch", Map.of("lat", 40.7489, "lon", -73.968),
                        "store", Map.of("lat", 40.7614, "lon", -73.9776)))));
  }

  @Test
  public void testMultipleOfficesWithGeoPoints() throws IOException {
    String query =
        String.format(
            "search source=%s | where id = '5' | fields multiple_offices", TEST_INDEX_COMPLEX_GEO);

    JSONObject result = executeQuery(query);
    verifySchema(result, schema("multiple_offices", null, "struct"));

    // Verify multiple offices structure
    verifyDataRows(
        result,
        rows(
            Map.of(
                "headquarters",
                    Map.of(
                        "location", Map.of("lat", 51.5074, "lon", -0.1278), "address", "London HQ"),
                "regional",
                    Map.of(
                        "location",
                        Map.of("lat", 48.8566, "lon", 2.3522),
                        "address",
                        "Paris Regional"))));
  }

  @Test
  public void testGeoHashInMap() throws IOException {
    String query =
        String.format(
            "search source=%s | where id = '6' | fields location", TEST_INDEX_COMPLEX_GEO);

    JSONObject result = executeQuery(query);
    verifySchema(result, schema("location", null, "struct"));

    // Verify geo point converted from geohash "u33dc0cpke7v"
    // Using tolerance for geohash conversion which has precision variations
    JSONArray dataRows = (JSONArray) result.get("datarows");
    JSONObject row = (JSONObject) ((JSONArray) dataRows.get(0)).get(0);

    assertEquals("Berlin Office", row.getString("name"));
    assertEquals("Berlin", row.getString("city"));
    assertEquals("Germany", row.getString("country"));

    JSONObject point = row.getJSONObject("point");
    double lat = point.getDouble("lat");
    double lon = point.getDouble("lon");

    // Expected values from geohash decoding with tolerance
    assertEquals(52.52003, lat, GeopointFormatsIT.TOLERANCE);
    assertEquals(13.40489, lon, GeopointFormatsIT.TOLERANCE);
  }

  @Test
  public void testComplexGeoAllDocumentsQuery() throws IOException {
    String query = String.format("search source=%s | fields id | sort id", TEST_INDEX_COMPLEX_GEO);

    JSONObject result = executeQuery(query);
    verifySchema(result, schema("id", null, "string"));

    // Verify all documents are indexed and queryable
    verifyDataRows(result, rows("1"), rows("2"), rows("3"), rows("4"), rows("5"), rows("6"));
  }
}

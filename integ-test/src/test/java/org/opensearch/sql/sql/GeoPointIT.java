/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.util.Map;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GEOPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

public class GeoPointIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.GEOPOINT);
  }

  @Test
  public void test_geo_point() {
    String query = "SELECT geo_point_object FROM " + TEST_INDEX_GEOPOINT;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows(new JSONObject(Map.of(
                "lat", 40.71,
                "lon", 74))),
        rows(new JSONObject(Map.of(
            "lat", -33.85253637358241,
            "lon", 151.21652352950258))),
        rows(JSONObject.NULL)
    );
  }

  @Test
  public void test_geo_point_unsupported_format() {
    String query = "SELECT geo_point_geohash FROM " + TEST_INDEX_GEOPOINT;
    Exception exception = assertThrows(RuntimeException.class,
        () -> executeJdbcRequest(query));

    assertTrue(exception.getMessage().contains(
            "  \"error\": {\n" +
            "    \"reason\": \"There was internal problem at backend\",\n" +
            "    \"details\": \"geo point must be in format of {\\\"lat\\\": number, \\\"lon\\\": number}\",\n" +
            "    \"type\": \"IllegalStateException\"\n" +
            "  }"
    ));
  }

  @Test
  public void test_geo_point_in_objects() {
    String query = "SELECT object.geo_point_object FROM " + TEST_INDEX_GEOPOINT;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows(
            (new JSONObject(Map.of(
                "lat", 40.71,
                "lon", 74)))),
        rows(new JSONObject(Map.of(
            "lat", -33.85253637358241,
            "lon", 151.21652352950258))),
        rows(JSONObject.NULL)
    );
  }

  @Test
  public void test_geo_point_lat_in_objects() {
    String query = "SELECT object.geo_point_object.lat FROM " + TEST_INDEX_GEOPOINT;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows(40.71),
        rows( -33.85253637358241),
        rows(JSONObject.NULL)
    );
  }

  @Test
  public void test_geo_point_lat_and_lon() {
    String query = "SELECT geo_point_object.lat, geo_point_object.lon FROM " + TEST_INDEX_GEOPOINT;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows(40.71, 74),
        rows(-33.85253637358241, 151.21652352950258),
        rows(JSONObject.NULL, JSONObject.NULL)
    );
  }

  @Test
  public void test_geo_point_object_with_lat_and_lon() {
    String query = "SELECT geo_point_object, geo_point_object.lat," +
        " geo_point_object.lon FROM " + TEST_INDEX_GEOPOINT;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows(new JSONObject(Map.of(
            "lat", 40.71,
            "lon", 74)),
            40.71, 74),
        rows(new JSONObject(Map.of(
            "lat", -33.85253637358241,
            "lon", 151.21652352950258)),
            -33.85253637358241, 151.21652352950258),
        rows(JSONObject.NULL, JSONObject.NULL, JSONObject.NULL)
    );
  }

  @Test
  public void test_geo_point_lat_in_functions() {
    String query = "SELECT ABS(geo_point_object.lat) FROM " + TEST_INDEX_GEOPOINT;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows(40.71),
        rows(33.85253637358241),
        rows(JSONObject.NULL)
    );
  }
}

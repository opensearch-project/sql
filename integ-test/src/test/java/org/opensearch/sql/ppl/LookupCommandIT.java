/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_IOT_READINGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_IOT_SENSORS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class LookupCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.IOT_READINGS);
    loadIndex(Index.IOT_SENSORS);
  }

  @Test
  public void testLookup() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | lookup %s did as device-id | sort @timestamp",
                TEST_INDEX_IOT_READINGS, TEST_INDEX_IOT_SENSORS));
    verifyDataRows(
        result,
        rows(
            28.1,
            "2015-01-20 15:31:32.406431",
            255,
            "temperature-basement",
            "meter",
            255,
            "VendorOne"),
        rows(
            27.8,
            "2016-01-20 15:31:33.509334",
            256,
            "temperature-living-room",
            "temperature meter",
            256,
            "VendorTwo"),
        rows(
            27.4,
            "2017-01-20 15:31:35.732436",
            257,
            "temperature-bedroom",
            "camcorder",
            257,
            "VendorThree"),
        rows(
            28.5,
            "2018-01-20 15:32:32.406431",
            255,
            "temperature-basement",
            "meter",
            255,
            "VendorOne"),
        rows(
            27.9,
            "2019-01-20 15:32:33.509334",
            256,
            "temperature-living-room",
            "temperature meter",
            256,
            "VendorTwo"),
        rows(
            27.4,
            "2020-01-20 15:32:35.732436",
            257,
            "temperature-bedroom",
            "camcorder",
            257,
            "VendorThree"));
  }

  @Test
  public void testLookupSelectedAttribute() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | lookup %s did as device-id type, vendor | sort @timestamp",
                TEST_INDEX_IOT_READINGS, TEST_INDEX_IOT_SENSORS));
    verifyDataRows(
        result,
        rows(28.1, "2015-01-20 15:31:32.406431", 255, "meter", "VendorOne"),
        rows(27.8, "2016-01-20 15:31:33.509334", 256, "temperature meter", "VendorTwo"),
        rows(27.4, "2017-01-20 15:31:35.732436", 257, "camcorder", "VendorThree"),
        rows(28.5, "2018-01-20 15:32:32.406431", 255, "meter", "VendorOne"),
        rows(27.9, "2019-01-20 15:32:33.509334", 256, "temperature meter", "VendorTwo"),
        rows(27.4, "2020-01-20 15:32:35.732436", 257, "camcorder", "VendorThree"));
  }

  @Test
  public void testLookupRenameSelectedAttributes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | lookup %s did as device-id  did as dev_id, type as kind, vendor | sort"
                    + " @timestamp",
                TEST_INDEX_IOT_READINGS, TEST_INDEX_IOT_SENSORS));
    verifyDataRows(
        result,
        rows(28.1, "2015-01-20 15:31:32.406431", 255, 255, "meter", "VendorOne"),
        rows(27.8, "2016-01-20 15:31:33.509334", 256, 256, "temperature meter", "VendorTwo"),
        rows(27.4, "2017-01-20 15:31:35.732436", 257, 257, "camcorder", "VendorThree"),
        rows(28.5, "2018-01-20 15:32:32.406431", 255, 255, "meter", "VendorOne"),
        rows(27.9, "2019-01-20 15:32:33.509334", 256, 256, "temperature meter", "VendorTwo"),
        rows(27.4, "2020-01-20 15:32:35.732436", 257, 257, "camcorder", "VendorThree"));
  }

  @Test
  public void testLookupSelectedMultipleAttributes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | lookup %s did as device-id type | sort @timestamp",
                TEST_INDEX_IOT_READINGS, TEST_INDEX_IOT_SENSORS));
    verifyDataRows(
        result,
        rows(28.1, "2015-01-20 15:31:32.406431", 255, "meter"),
        rows(27.8, "2016-01-20 15:31:33.509334", 256, "temperature meter"),
        rows(27.4, "2017-01-20 15:31:35.732436", 257, "camcorder"),
        rows(28.5, "2018-01-20 15:32:32.406431", 255, "meter"),
        rows(27.9, "2019-01-20 15:32:33.509334", 256, "temperature meter"),
        rows(27.4, "2020-01-20 15:32:35.732436", 257, "camcorder"));
  }

  @Test
  public void testLookupShouldAppendOnlyShouldBeFalseByDefault() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rename temperature as vendor | lookup %s did as device-id | sort"
                    + " @timestamp",
                TEST_INDEX_IOT_READINGS, TEST_INDEX_IOT_SENSORS));
    verifyDataRows(
        result,
        rows("2015-01-20 15:31:32.406431", 255, "VendorOne", "temperature-basement", "meter", 255),
        rows(
            "2016-01-20 15:31:33.509334",
            256,
            "VendorTwo",
            "temperature-living-room",
            "temperature meter",
            256),
        rows(
            "2017-01-20 15:31:35.732436",
            257,
            "VendorThree",
            "temperature-bedroom",
            "camcorder",
            257),
        rows("2018-01-20 15:32:32.406431", 255, "VendorOne", "temperature-basement", "meter", 255),
        rows(
            "2019-01-20 15:32:33.509334",
            256,
            "VendorTwo",
            "temperature-living-room",
            "temperature meter",
            256),
        rows(
            "2020-01-20 15:32:35.732436",
            257,
            "VendorThree",
            "temperature-bedroom",
            "camcorder",
            257));
  }

  @Test
  public void testLookupWithAppendOnlyFalse() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rename temperature as vendor | lookup %s did as device-id appendonly ="
                    + " true | sort @timestamp",
                TEST_INDEX_IOT_READINGS, TEST_INDEX_IOT_SENSORS));
    verifyDataRows(
        result,
        rows("2015-01-20 15:31:32.406431", 255, 28.1, "temperature-basement", "meter", 255),
        rows(
            "2016-01-20 15:31:33.509334",
            256,
            27.8,
            "temperature-living-room",
            "temperature meter",
            256),
        rows("2017-01-20 15:31:35.732436", 257, 27.4, "temperature-bedroom", "camcorder", 257),
        rows("2018-01-20 15:32:32.406431", 255, 28.5, "temperature-basement", "meter", 255),
        rows(
            "2019-01-20 15:32:33.509334",
            256,
            27.9,
            "temperature-living-room",
            "temperature meter",
            256),
        rows("2020-01-20 15:32:35.732436", 257, 27.4, "temperature-bedroom", "camcorder", 257));
  }
}

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
import java.math.BigDecimal;
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
            new BigDecimal("28.1"),
            255,
            "2015-01-20 15:31:32.406431",
            "temperature-basement",
            "meter",
            255,
            "VendorOne"),
        rows(
            new BigDecimal("27.8"),
            256,
            "2016-01-20 15:31:33.509334",
            "temperature-living-room",
            "temperature meter",
            256,
            "VendorTwo"),
        rows(
            new BigDecimal("27.4"),
            257,
            "2017-01-20 15:31:35.732436",
            "temperature-bedroom",
            "camcorder",
            257,
            "VendorThree"),
        rows(
            new BigDecimal("28.5"),
            255,
            "2018-01-20 15:32:32.406431",
            "temperature-basement",
            "meter",
            255,
            "VendorOne"),
        rows(
            new BigDecimal("27.9"),
            256,
            "2019-01-20 15:32:33.509334",
            "temperature-living-room",
            "temperature meter",
            256,
            "VendorTwo"),
        rows(
            new BigDecimal("27.4"),
            257,
            "2020-01-20 15:32:35.732436",
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
        rows(new BigDecimal("28.1"), 255, "2015-01-20 15:31:32.406431", "meter", "VendorOne"),
        rows(
            new BigDecimal("27.8"),
            256,
            "2016-01-20 15:31:33.509334",
            "temperature meter",
            "VendorTwo"),
        rows(new BigDecimal("27.4"), 257, "2017-01-20 15:31:35.732436", "camcorder", "VendorThree"),
        rows(new BigDecimal("28.5"), 255, "2018-01-20 15:32:32.406431", "meter", "VendorOne"),
        rows(
            new BigDecimal("27.9"),
            256,
            "2019-01-20 15:32:33.509334",
            "temperature meter",
            "VendorTwo"),
        rows(
            new BigDecimal("27.4"), 257, "2020-01-20 15:32:35.732436", "camcorder", "VendorThree"));
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
        rows(new BigDecimal("28.1"), 255, "2015-01-20 15:31:32.406431", 255, "meter", "VendorOne"),
        rows(
            new BigDecimal("27.8"),
            256,
            "2016-01-20 15:31:33.509334",
            256,
            "temperature meter",
            "VendorTwo"),
        rows(
            new BigDecimal("27.4"),
            257,
            "2017-01-20 15:31:35.732436",
            257,
            "camcorder",
            "VendorThree"),
        rows(new BigDecimal("28.5"), 255, "2018-01-20 15:32:32.406431", 255, "meter", "VendorOne"),
        rows(
            new BigDecimal("27.9"),
            256,
            "2019-01-20 15:32:33.509334",
            256,
            "temperature meter",
            "VendorTwo"),
        rows(
            new BigDecimal("27.4"),
            257,
            "2020-01-20 15:32:35.732436",
            257,
            "camcorder",
            "VendorThree"));
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
        rows(new BigDecimal("28.1"), 255, "2015-01-20 15:31:32.406431", "meter"),
        rows(new BigDecimal("27.8"), 256, "2016-01-20 15:31:33.509334", "temperature meter"),
        rows(new BigDecimal("27.4"), 257, "2017-01-20 15:31:35.732436", "camcorder"),
        rows(new BigDecimal("28.5"), 255, "2018-01-20 15:32:32.406431", "meter"),
        rows(new BigDecimal("27.9"), 256, "2019-01-20 15:32:33.509334", "temperature meter"),
        rows(new BigDecimal("27.4"), 257, "2020-01-20 15:32:35.732436", "camcorder"));
  }

  @Test
  public void testLookupShouldOverwriteShouldBeFalseByDefault() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rename temperature as vendor | lookup %s did as device-id | sort"
                    + " @timestamp",
                TEST_INDEX_IOT_READINGS, TEST_INDEX_IOT_SENSORS));
    verifyDataRows(
        result,
        rows(255, "2015-01-20 15:31:32.406431", "VendorOne", "temperature-basement", "meter", 255),
        rows(
            256,
            "2016-01-20 15:31:33.509334",
            "VendorTwo",
            "temperature-living-room",
            "temperature meter",
            256),
        rows(
            257,
            "2017-01-20 15:31:35.732436",
            "VendorThree",
            "temperature-bedroom",
            "camcorder",
            257),
        rows(255, "2018-01-20 15:32:32.406431", "VendorOne", "temperature-basement", "meter", 255),
        rows(
            256,
            "2019-01-20 15:32:33.509334",
            "VendorTwo",
            "temperature-living-room",
            "temperature meter",
            256),
        rows(
            257,
            "2020-01-20 15:32:35.732436",
            "VendorThree",
            "temperature-bedroom",
            "camcorder",
            257));
  }

  @Test
  public void testLookupWithOverwriteFalse() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | rename temperature as vendor | lookup %s did as device-id overwrite ="
                    + " true | sort @timestamp",
                TEST_INDEX_IOT_READINGS, TEST_INDEX_IOT_SENSORS));
    verifyDataRows(
        result,
        rows(
            255,
            "2015-01-20 15:31:32.406431",
            new BigDecimal("28.1"),
            "temperature-basement",
            "meter",
            255),
        rows(
            256,
            "2016-01-20 15:31:33.509334",
            new BigDecimal("27.8"),
            "temperature-living-room",
            "temperature meter",
            256),
        rows(
            257,
            "2017-01-20 15:31:35.732436",
            new BigDecimal("27.4"),
            "temperature-bedroom",
            "camcorder",
            257),
        rows(
            255,
            "2018-01-20 15:32:32.406431",
            new BigDecimal("28.5"),
            "temperature-basement",
            "meter",
            255),
        rows(
            256,
            "2019-01-20 15:32:33.509334",
            new BigDecimal("27.9"),
            "temperature-living-room",
            "temperature meter",
            256),
        rows(
            257,
            "2020-01-20 15:32:35.732436",
            new BigDecimal("27.4"),
            "temperature-bedroom",
            "camcorder",
            257));
  }
}

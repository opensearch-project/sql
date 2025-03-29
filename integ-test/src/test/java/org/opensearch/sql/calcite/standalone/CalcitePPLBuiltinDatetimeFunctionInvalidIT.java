/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS_WITH_NULL;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class CalcitePPLBuiltinDatetimeFunctionInvalidIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(SQLIntegTestCase.Index.STATE_COUNTRY);
    loadIndex(SQLIntegTestCase.Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(SQLIntegTestCase.Index.DATE_FORMATS);
    loadIndex(SQLIntegTestCase.Index.DATE_FORMATS_WITH_NULL);
  }

  @Test
  public void testYearWeekInvalid() {
    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  | eval `YEARWEEK('2020-08-26')` = YEARWEEK('2020-15-26')",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testYearInvalid() {
    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  | eval a = YEAR('2020-15-26')",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  | eval a = YEAR('2020-12-26 25:00:00')",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testWeekInvalid() {
    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  | eval a = WEEK('2020-15-26')",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  | eval a = WEEK('2020-12-26 25:00:00')",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testTO_SECONDSInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TO_SECONDS('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TO_SECONDS('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TO_SECONDS('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDATEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testTIMEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDAYInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDAYNAMEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYNAME('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYNAME('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYNAME('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDAYOFMONTHInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFMONTH('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFMONTH('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFMONTH('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDAY_OF_MONTHInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_MONTH('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_MONTH('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_MONTH('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDAYOFWEEKInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFWEEK('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFWEEK('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFWEEK('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFWEEK('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFWEEK('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFWEEK('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDAY_OF_WEEKInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_WEEK('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_WEEK('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_WEEK('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDAYOFYEARInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFYEAR('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFYEAR('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAYOFYEAR('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDAY_OF_YEARInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_YEAR('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_YEAR('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DAY_OF_YEAR('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testHOURInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=HOUR('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=HOUR('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=HOUR('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testHOUR_OF_DAYInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=HOUR_OF_DAY('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=HOUR_OF_DAY('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=HOUR_OF_DAY('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testLAST_DAYInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=LAST_DAY('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=LAST_DAY('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=LAST_DAY('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testMINUTEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testMINUTE_OF_DAYInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE_OF_DAY('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE_OF_DAY('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE_OF_DAY('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testMINUTE_OF_HOURInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE_OF_HOUR('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE_OF_HOUR('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MINUTE_OF_HOUR('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testMONTHInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTH('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTH('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTH('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testMONTH_OF_YEARInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTH_OF_YEAR('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTH_OF_YEAR('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTH_OF_YEAR('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testMONTHNAMEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTHNAME('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTHNAME('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=MONTHNAME('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testQUARTERInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=QUARTER('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=QUARTER('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=QUARTER('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testSECONDInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SECOND('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SECOND('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SECOND('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testSECOND_OF_MINUTEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SECOND_OF_MINUTE('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SECOND_OF_MINUTE('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SECOND_OF_MINUTE('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testTIME_TO_SECInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME_TO_SEC('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME_TO_SEC('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME_TO_SEC('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testTIMESTAMPInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('2025-13-02', '2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('16:00:61', '16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMP('2025-12-01 15:02:61', '2025-12-01 15:02:61')"
                          + " | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testTO_DAYSInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TO_DAYS('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TO_DAYS('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TO_DAYS('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testYEARInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=YEAR('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=YEAR('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=YEAR('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testWEEKInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEK('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEK('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEK('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testWEEK_OF_YEARInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEK_OF_YEAR('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEK_OF_YEAR('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEK_OF_YEAR('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testWEEKDAYInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEKDAY('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEKDAY('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=WEEKDAY('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testYEARWEEKInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=YEARWEEK('2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=YEARWEEK('16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=YEARWEEK('2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testADDTATEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTATE('2025-13-02', INTERVAL 1 HOUR) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTATE('16:00:61', INTERVAL 1 HOUR) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTATE('2025-12-01 15:02:61', INTERVAL 1 HOUR) |"
                          + " fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTATE('2025-13-02', 1) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTATE('16:00:61', 1) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTATE('2025-12-01 15:02:61', 1) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testADDTIMEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTIME('2025-13-02', '2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTIME('16:00:61', '16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=ADDTIME('2025-12-01 15:02:61', '2025-12-01 15:02:61') |"
                          + " fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDATE_ADDInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_ADD('2025-13-02', INTERVAL 1 HOUR) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_ADD('16:00:61', INTERVAL 1 HOUR) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_ADD('2025-12-01 15:02:61', INTERVAL 1 HOUR) |"
                          + " fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDATE_SUBInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_SUB('2025-13-02', INTERVAL 1 HOUR) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_SUB('16:00:61', INTERVAL 1 HOUR) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_SUB('2025-12-01 15:02:61', INTERVAL 1 HOUR) |"
                          + " fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDATEDIFFInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATEDIFF('2025-13-02', '2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATEDIFF('16:00:61', '16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATEDIFF('2025-12-01 15:02:61', '2025-12-01 15:02:61')"
                          + " | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testSUBDATEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBDATE('2025-13-02', INTERVAL 1 HOUR) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBDATE('16:00:61', INTERVAL 1 HOUR) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBDATE('2025-12-01 15:02:61', INTERVAL 1 HOUR) |"
                          + " fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBDATE('2025-13-02', 1) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBDATE('16:00:61', 1) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBDATE('2025-12-01 15:02:61', 1) | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testSUBTIMEInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBTIME('2025-13-02', '2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBTIME('16:00:61', '16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=SUBTIME('2025-12-01 15:02:61', '2025-12-01 15:02:61') |"
                          + " fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testTIMESTAMPADDInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMPADD(INTERVAL 1 HOUR, 1, '2025-13-02') | fields"
                          + " a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMPADD(INTERVAL 1 HOUR, 1, '16:00:61') | fields"
                          + " a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMPADD(INTERVAL 1 HOUR, 1, '2025-12-01 15:02:61')"
                          + " | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testTIMESTAMPDIFFInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMPDIFF(INTERVAL 1 HOUR, '2025-13-02',"
                          + " '2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMPDIFF(INTERVAL 1 HOUR, '16:00:61', '16:00:61')"
                          + " | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIMESTAMPDIFF(INTERVAL 1 HOUR, '2025-12-01 15:02:61',"
                          + " '2025-12-01 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testDATE_FORMATInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_FORMAT('2025-13-02', '2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_FORMAT('16:00:61', '16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=DATE_FORMAT('2025-12-01 15:02:61', '2025-12-01"
                          + " 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }

  @Test
  public void testTIME_FORMATInvalid() {

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME_FORMAT('2025-13-02', '2025-13-02') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME_FORMAT('16:00:61', '16:00:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

    assertThrows(
        Exception.class,
        () -> {
          JSONObject actual =
              executeQuery(
                  String.format(
                      "source=%s  |  eval a=TIME_FORMAT('2025-12-01 15:02:61', '2025-12-01"
                          + " 15:02:61') | fields a",
                      TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
  }
}

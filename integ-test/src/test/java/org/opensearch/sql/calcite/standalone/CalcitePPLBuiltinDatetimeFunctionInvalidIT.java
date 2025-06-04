/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
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
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  | eval `YEARWEEK('2020-08-26')` = YEARWEEK('2020-15-26')",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e, "unsupported format");
  }

  @Test
  public void testYearInvalid() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  | eval a = YEAR('2020-15-26')",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e, "unsupported format");
    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  | eval a = YEAR('2020-12-26 25:00:00')",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");
  }

  @Test
  public void testWeekInvalid() {
    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  | eval a = WEEK('2020-15-26')",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  | eval a = WEEK('2020-12-26 25:00:00')",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");
  }

  @Test
  public void testTO_SECONDSInvalid() {
    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TO_SECONDS('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TO_SECONDS('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");
    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TO_SECONDS('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testDATEInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");
    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");
    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testTIMEInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testDAYInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testDAYNAMEInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYNAME('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(
        e1, "date:2025-13-02 in unsupported format, please use 'yyyy-MM-dd'");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYNAME('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "date:16:00:61 in unsupported format, please use 'yyyy-MM-dd'");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYNAME('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(
        e3, "date:2025-12-01 15:02:61 in unsupported format, please use 'yyyy-MM-dd'");
  }

  @Test
  public void testDAYOFMONTHInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFMONTH('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFMONTH('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "date:16:00:61 in unsupported format, please use 'yyyy-MM-dd'");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFMONTH('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(
        e3, "date:2025-12-01 15:02:61 in unsupported format, please use 'yyyy-MM-dd'");
  }

  @Test
  public void testDAY_OF_MONTHInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_MONTH('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(
        e1, "date:2025-13-02 in unsupported format, please use 'yyyy-MM-dd'");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_MONTH('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_MONTH('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testDAYOFWEEKInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFWEEK('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFWEEK('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFWEEK('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");

    SemanticCheckException e4 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFWEEK('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e4, "unsupported format");

    SemanticCheckException e5 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFWEEK('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e5, "unsupported format");

    SemanticCheckException e6 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFWEEK('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e6, "unsupported format");
  }

  @Test
  public void testDAY_OF_WEEKInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_WEEK('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_WEEK('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_WEEK('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testDAYOFYEARInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFYEAR('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFYEAR('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAYOFYEAR('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testDAY_OF_YEARInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_YEAR('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_YEAR('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DAY_OF_YEAR('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testHOURInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=HOUR('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=HOUR('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=HOUR('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testHOUR_OF_DAYInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=HOUR_OF_DAY('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=HOUR_OF_DAY('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=HOUR_OF_DAY('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testLAST_DAYInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=LAST_DAY('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=LAST_DAY('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=LAST_DAY('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testMINUTEInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testMINUTE_OF_DAYInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE_OF_DAY('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE_OF_DAY('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE_OF_DAY('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testMINUTE_OF_HOURInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE_OF_HOUR('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE_OF_HOUR('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MINUTE_OF_HOUR('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testMONTHInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTH('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTH('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTH('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testMONTH_OF_YEARInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTH_OF_YEAR('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTH_OF_YEAR('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTH_OF_YEAR('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testMONTHNAMEInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTHNAME('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(
        e1, "date:2025-13-02 in unsupported format, please use 'yyyy-MM-dd'");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTHNAME('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "date:16:00:61 in unsupported format, please use 'yyyy-MM-dd'");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=MONTHNAME('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(
        e3, "date:2025-12-01 15:02:61 in unsupported format, please use 'yyyy-MM-dd'");
  }

  @Test
  public void testQUARTERInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=QUARTER('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=QUARTER('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=QUARTER('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testSECONDInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SECOND('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SECOND('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SECOND('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testSECOND_OF_MINUTEInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SECOND_OF_MINUTE('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SECOND_OF_MINUTE('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SECOND_OF_MINUTE('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testTIME_TO_SECInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME_TO_SEC('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME_TO_SEC('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME_TO_SEC('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testTIMESTAMPInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");

    SemanticCheckException e4 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e4, "unsupported format");

    SemanticCheckException e5 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e5, "unsupported format");

    SemanticCheckException e6 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e6, "unsupported format");

    SemanticCheckException e7 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('2025-13-02', '2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e7, "unsupported format");

    SemanticCheckException e8 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('16:00:61', '16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e8, "unsupported format");

    SemanticCheckException e9 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMP('2025-12-01 15:02:61', '2025-12-01"
                            + " 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e9, "unsupported format");
  }

  @Test
  public void testTO_DAYSInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TO_DAYS('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TO_DAYS('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TO_DAYS('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testYEARInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=YEAR('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=YEAR('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=YEAR('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testWEEKInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEK('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEK('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEK('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testWEEK_OF_YEARInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEK_OF_YEAR('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEK_OF_YEAR('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEK_OF_YEAR('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testWEEKDAYInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEKDAY('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEKDAY('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=WEEKDAY('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testYEARWEEKInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=YEARWEEK('2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=YEARWEEK('16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=YEARWEEK('2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testADDTATEInvalid() {

    ExpressionEvaluationException e1 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDDATE('2025-13-02', INTERVAL 1 HOUR) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e2 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDDATE('16:00:61', INTERVAL 1 HOUR) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e3 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDDATE('2025-12-01 15:02:61', INTERVAL 1 HOUR) |"
                            + " fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e4 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDDATE('2025-13-02', 1) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e4, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e5 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDDATE('16:00:61', 1) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e5, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e6 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDDATE('2025-12-01 15:02:61', 1) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e6, "invalid to get timestampValue from value of type STRING");
  }

  @Test
  public void testADDTIMEInvalid() {

    Throwable e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDTIME('2025-13-02', '2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    Throwable e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDTIME('16:00:61', '16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    Throwable e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=ADDTIME('2025-12-01 15:02:61', '2025-12-01"
                            + " 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testDATE_ADDInvalid() {

    ExpressionEvaluationException e1 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_ADD('2025-13-02', INTERVAL 1 HOUR) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e2 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_ADD('16:00:61', INTERVAL 1 HOUR) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e3 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_ADD('2025-12-01 15:02:61', INTERVAL 1 HOUR) |"
                            + " fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "invalid to get timestampValue from value of type STRING");
  }

  @Test
  public void testDATE_SUBInvalid() {

    ExpressionEvaluationException e1 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_SUB('2025-13-02', INTERVAL 1 HOUR) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e2 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_SUB('16:00:61', INTERVAL 1 HOUR) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e3 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_SUB('2025-12-01 15:02:61', INTERVAL 1 HOUR) |"
                            + " fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "invalid to get timestampValue from value of type STRING");
  }

  @Test
  public void testDATEDIFFInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATEDIFF('2025-13-02', '2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATEDIFF('16:00:61', '16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATEDIFF('2025-12-01 15:02:61', '2025-12-01"
                            + " 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testSUBDATEInvalid() {

    ExpressionEvaluationException e1 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBDATE('2025-13-02', INTERVAL 1 HOUR) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e2 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBDATE('16:00:61', INTERVAL 1 HOUR) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e3 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBDATE('2025-12-01 15:02:61', INTERVAL 1 HOUR) |"
                            + " fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e4 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBDATE('2025-13-02', 1) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e4, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e5 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBDATE('16:00:61', 1) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e5, "invalid to get timestampValue from value of type STRING");

    ExpressionEvaluationException e6 =
        assertThrows(
                ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBDATE('2025-12-01 15:02:61', 1) | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e6, "invalid to get timestampValue from value of type STRING");
  }

  @Test
  public void testSUBTIMEInvalid() {
    Throwable e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBTIME('2025-13-02', '2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    Throwable e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBTIME('16:00:61', '16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    Throwable e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=SUBTIME('2025-12-01 15:02:61', '2025-12-01"
                            + " 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testTIMESTAMPADDInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMPADD(HOUR, 1, '2025-13-02') | fields" + " a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMPADD(HOUR, 1, '16:00:61') | fields" + " a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMPADD(HOUR, 1, '2025-12-01 15:02:61')"
                            + " | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testTIMESTAMPDIFFInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMPDIFF(HOUR, '2025-13-02',"
                            + " '2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMPDIFF(HOUR, '16:00:61', '16:00:61')"
                            + " | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIMESTAMPDIFF(HOUR, '2025-12-01 15:02:61',"
                            + " '2025-12-01 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testDATE_FORMATInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_FORMAT('2025-13-02', '2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_FORMAT('16:00:61', '16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=DATE_FORMAT('2025-12-01 15:02:61', '2025-12-01"
                            + " 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }

  @Test
  public void testTIME_FORMATInvalid() {

    SemanticCheckException e1 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME_FORMAT('2025-13-02', '2025-13-02') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e1, "unsupported format");

    SemanticCheckException e2 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME_FORMAT('16:00:61', '16:00:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e2, "unsupported format");

    SemanticCheckException e3 =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s  |  eval a=TIME_FORMAT('2025-12-01 15:02:61', '2025-12-01"
                            + " 15:02:61') | fields a",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    verifyErrorMessageContains(e3, "unsupported format");
  }
}

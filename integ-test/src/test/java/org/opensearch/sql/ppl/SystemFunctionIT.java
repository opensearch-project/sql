/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.DATA_TYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class SystemFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(DATA_TYPE_NUMERIC);
    loadIndex(DATA_TYPE_NONNUMERIC);
  }

  @Test
  public void typeof_sql_types() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | eval `str` = typeof('pewpew'),"
                    + " `double` = typeof(1.0),"
                    + "`int` = typeof(12345),"
                    + " `long` = typeof(1234567891011),"
                    + " `interval` = typeof(INTERVAL 2 DAY)"
                    + "  | fields `str`, `double`, `int`, `long`, `interval`",
                TEST_INDEX_DATATYPE_NUMERIC));
    // TODO: test null in PPL
    verifyDataRows(response, rows("KEYWORD", "DOUBLE", "INTEGER", "LONG", "INTERVAL"));

    response =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + "`timestamp` = typeof(CAST('1961-04-12 09:07:00' AS TIMESTAMP)),"
                    + "`time` = typeof(CAST('09:07:00' AS TIME)),"
                    + "`date` = typeof(CAST('1961-04-12' AS DATE)),"
                    + "`datetime` = typeof(DATETIME('1961-04-12 09:07:00'))"
                    + " | fields `timestamp`, `time`, `date`, `datetime`",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(response, rows("TIMESTAMP", "TIME", "DATE", "DATETIME"));
  }

  @Test
  public void typeof_opensearch_types() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | eval `double` = typeof(double_number), `long` ="
                    + " typeof(long_number),`integer` = typeof(integer_number), `byte` ="
                    + " typeof(byte_number),`short` = typeof(short_number), `float` ="
                    + " typeof(float_number),`half_float` = typeof(half_float_number),"
                    + " `scaled_float` = typeof(scaled_float_number) | fields `double`, `long`,"
                    + " `integer`, `byte`, `short`, `float`, `half_float`, `scaled_float`",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(
        response, rows("DOUBLE", "LONG", "INTEGER", "BYTE", "SHORT", "FLOAT", "FLOAT", "DOUBLE"));

    response =
        executeQuery(
            String.format(
                "source=%s | eval `text` = typeof(text_value), `date` = typeof(date_value),"
                    + " `date_nanos` = typeof(date_nanos_value),`boolean` = typeof(boolean_value),"
                    + " `object` = typeof(object_value),`keyword` = typeof(keyword_value), `ip` ="
                    + " typeof(ip_value),`binary` = typeof(binary_value), `geo_point` ="
                    + " typeof(geo_point_value)"
                    // TODO activate this test once `ARRAY` type supported, see
                    // ExpressionAnalyzer::isTypeNotSupported
                    // + ", `nested` = typeof(nested_value)"
                    + " | fields `text`, `date`, `date_nanos`, `boolean`, `object`, `keyword`,"
                    + " `ip`, `binary`, `geo_point`",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifyDataRows(
        response,
        rows(
            "TEXT",
            "TIMESTAMP",
            "TIMESTAMP",
            "BOOLEAN",
            "OBJECT",
            "KEYWORD",
            "IP",
            "BINARY",
            "GEO_POINT"));
  }
}

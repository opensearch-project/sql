/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.calcite.remote.fallback.CalciteSystemFunctionIT;

public class NonFallbackCalciteSystemFunctionIT extends CalciteSystemFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
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
    // TODO https://github.com/opensearch-project/sql/issues/3322
    // TO support text and IP, we need support UDT.
    verifyDataRows(
        response,
        rows(
            "STRING",
            "TIMESTAMP",
            "TIMESTAMP",
            "BOOLEAN",
            "STRUCT",
            "STRING",
            "KEYWORD", // should be IP
            "BINARY",
            "GEO_POINT"));
  }
}

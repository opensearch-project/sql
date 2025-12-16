/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class ConversionFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testDecimal() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s |head 1| eval a = tonumber('4598.678')  | fields a",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("a", "double"));

    verifyDataRows(actual, rows(4598.678));
  }

  @Test
  public void testHex() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s |head 1| eval a = tonumber('FF12CA',16)  | fields a",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("a", "double"));

    verifyDataRows(actual, rows(16716490));
  }

  @Test
  public void testBinary() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s |head 1| eval a = tonumber('0110111',2)  | fields a",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("a", "double"));

    verifyDataRows(actual, rows(55));
  }

  @Test
  public void testOctal() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s |head 1| eval a = tonumber('20415442',8)  | fields a",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("a", "double"));

    verifyDataRows(actual, rows(4332322));
  }

  @Test
  public void testOctalWithUnsupportedValue() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s |head 1| eval a = tonumber('20415.442',8)  | fields a",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("a", "double"));

    assertEquals(actual.getJSONArray("datarows").getJSONArray(0).get(0), null);
  }

  @Test
  public void testBinaryWithUnsupportedValue() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s |head 1| eval a = tonumber('1010.11',2)  | fields a",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("a", "double"));

    assertEquals(actual.getJSONArray("datarows").getJSONArray(0).get(0), null);
  }

  @Test
  public void testHexWithUnsupportedValue() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s |head 1| eval a = tonumber('A.B',16)  | fields a", TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("a", "double"));

    assertEquals(actual.getJSONArray("datarows").getJSONArray(0).get(0), null);
  }
}

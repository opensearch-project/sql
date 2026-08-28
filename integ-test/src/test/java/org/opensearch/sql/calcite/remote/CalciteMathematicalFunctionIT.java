/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.MathematicalFunctionIT;

public class CalciteMathematicalFunctionIT extends MathematicalFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testRoundWithArithmeticPrecision() throws IOException {
    // ROUND with arithmetic precision argument. PPL arithmetic widens to BIGINT but ROUND
    // expects int.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = round(123.456, 1 + 0) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", null, "double"));
    verifyDataRows(actual, rows(123.5));
  }

  @Test
  public void testTruncateWithArithmeticPrecision() throws IOException {
    // TRUNCATE with arithmetic precision argument. PPL arithmetic widens to BIGINT but TRUNCATE
    // expects int.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = truncate(123.456, 1 + 0) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", null, "double"));
    verifyDataRows(actual, rows(123.4));
  }

  @Test
  public void testRoundWithCastLongPrecision() throws IOException {
    // ROUND with an explicit cast(x as long) precision. Pre-existing bug: a genuinely-BIGINT
    // value handed to ROUND's int parameter must be narrowed to INTEGER.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = round(123.456, cast(1 as long)) | head 1 | fields"
                    + " result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", null, "double"));
    verifyDataRows(actual, rows(123.5));
  }

  @Test
  public void testConvWithArithmeticBases() throws IOException {
    // CONV with arithmetic base arguments. Both widen to BIGINT but CONV expects int radixes.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = conv('11', 1 + 1, 8 + 2) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", null, "string"));
    verifyDataRows(actual, rows("3"));
  }

  @Test
  public void testSha2WithArithmeticBitLength() throws IOException {
    // SHA2 with arithmetic bit-length argument. Widens to BIGINT but SHA2 expects int.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval result = sha2('abc', 128 + 128) | head 1 | fields result",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("result", null, "string"));
  }
}

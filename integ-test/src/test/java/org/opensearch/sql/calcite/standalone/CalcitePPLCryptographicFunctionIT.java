/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLCryptographicFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.PEOPLE);
  }

  @Test
  public void testMd5() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval hello = MD5('hello') | fields hello",
                TEST_INDEX_PEOPLE));
    verifySchema(actual, schema("hello", "string"));
    verifyDataRows(actual, rows("5d41402abc4b2a76b9719d911017c592"));
  }

  @Test
  public void testSha1() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval hello = SHA1('hello') | fields hello",
                TEST_INDEX_PEOPLE));
    verifySchema(actual, schema("hello", "string"));
    verifyDataRows(actual, rows("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"));
  }

  @Test
  public void testSha2() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval sha256 = SHA2('hello',256), sha512 = SHA2('hello',512) "
                    + " | fields sha256, sha512",
                TEST_INDEX_PEOPLE));
    verifySchema(actual, schema("sha256", "string"), schema("sha512", "string"));
    verifyDataRows(
        actual,
        rows(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"));
  }

  @Test
  public void testSha2WrongAlgorithmShouldThrow() {
    Throwable e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | head 1 | eval sha100 = SHA2('hello', 100) | fields sha100",
                        TEST_INDEX_PEOPLE)));
    verifyErrorMessageContains(e, "Unsupported SHA2 algorithm");
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class CalcitePPLCryptographicFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testMd5() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name = 'Jake' | eval hello = MD5('hello'), california ="
                    + " md5(state) | fields hello, california",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(actual, schema("hello", "string"), schema("california", "string"));
    verifyDataRows(
        actual, rows("5d41402abc4b2a76b9719d911017c592", "356779a9a1696714480f57fa3fb66d4c"));
  }

  @Test
  public void testSha1() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name = 'John' | eval hello = SHA1('hello'), ontario ="
                    + " SHA1(state) | fields hello, ontario",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(actual, schema("hello", "string"), schema("ontario", "string"));
    verifyDataRows(
        actual,
        rows(
            "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
            "f9f742e1f653a74c4cd78d7ea283b5556539b96b"));
  }

  @Test
  public void testSha2() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name = 'Jane' | eval sha256 = SHA2('hello',256), sha512 ="
                    + " SHA2('hello',512), sha224 = SHA2(country, 224), sha384 = SHA2(country, 384)"
                    + " | fields sha256, sha512, sha224, sha384",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("sha256", "string"),
        schema("sha512", "string"),
        schema("sha224", "string"),
        schema("sha384", "string"));
    verifyDataRows(
        actual,
        rows(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043",
            "c16f747ca3d2e267c76e7355429fb1583268d966887f237b8e1605c7",
            "de2abcb28b87d681830f3af25cd8dde7fdc2a4da9dcfde60b371fd2378a70ac39cef3e104bbe09aecda022aee7b4bf59"));
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
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "Unsupported SHA2 algorithm");
  }

  @Test
  public void testSha2WrongArgShouldThrow() {
    Throwable e =
        assertThrows(
            ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | head 1 | eval sha256 = SHA2('hello', '256') | fields sha256",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(
        e, "SHA2 function expects {[STRING,INTEGER]}, but got [STRING,STRING]");
  }
}

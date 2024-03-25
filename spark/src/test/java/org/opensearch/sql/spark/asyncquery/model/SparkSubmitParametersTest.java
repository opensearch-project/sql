/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class SparkSubmitParametersTest {

  @Test
  public void testBuildWithoutExtraParameters() {
    String params = SparkSubmitParameters.Builder.builder().build().toString();

    assertNotNull(params);
  }

  @Test
  public void testBuildWithExtraParameters() {
    String params =
        SparkSubmitParameters.Builder.builder().extraParameters("--conf A=1").build().toString();

    // Assert the conf is included with a space
    assertTrue(params.endsWith(" --conf A=1"));
  }

  @Test
  public void testBuildQueryString() {
    String rawQuery = "SHOW tables LIKE \"%\";";
    String expectedQueryInParams = "\"SHOW tables LIKE \\\"%\\\";\"";
    String params = SparkSubmitParameters.Builder.builder().query(rawQuery).build().toString();
    assertTrue(params.contains(expectedQueryInParams));
  }

  @Test
  public void testBuildQueryStringNestedQuote() {
    String rawQuery = "SELECT '\"1\"'";
    String expectedQueryInParams = "\"SELECT '\\\"1\\\"'\"";
    String params = SparkSubmitParameters.Builder.builder().query(rawQuery).build().toString();
    assertTrue(params.contains(expectedQueryInParams));
  }

  @Test
  public void testBuildQueryStringSpecialCharacter() {
    String rawQuery = "SELECT '{\"test ,:+\\\"inner\\\"/\\|?#><\"}'";
    String expectedQueryInParams = "SELECT '{\\\"test ,:+\\\\\\\"inner\\\\\\\"/\\\\|?#><\\\"}'";
    String params = SparkSubmitParameters.Builder.builder().query(rawQuery).build().toString();
    assertTrue(params.contains(expectedQueryInParams));
  }
}

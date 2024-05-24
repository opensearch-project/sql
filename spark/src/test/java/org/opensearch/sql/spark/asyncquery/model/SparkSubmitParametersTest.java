/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JARS_KEY;

import org.junit.jupiter.api.Test;

public class SparkSubmitParametersTest {

  @Test
  public void testBuildWithoutExtraParameters() {
    String params = SparkSubmitParameters.builder().build().toString();

    assertNotNull(params);
  }

  @Test
  public void testBuildWithExtraParameters() {
    String params =
        SparkSubmitParameters.builder().extraParameters("--conf A=1").build().toString();

    // Assert the conf is included with a space
    assertTrue(params.endsWith(" --conf A=1"));
  }

  @Test
  public void testBuildQueryString() {
    String rawQuery = "SHOW tables LIKE \"%\";";
    String expectedQueryInParams = "\"SHOW tables LIKE \\\"%\\\";\"";
    String params = SparkSubmitParameters.builder().query(rawQuery).build().toString();
    assertTrue(params.contains(expectedQueryInParams));
  }

  @Test
  public void testBuildQueryStringNestedQuote() {
    String rawQuery = "SELECT '\"1\"'";
    String expectedQueryInParams = "\"SELECT '\\\"1\\\"'\"";
    String params = SparkSubmitParameters.builder().query(rawQuery).build().toString();
    assertTrue(params.contains(expectedQueryInParams));
  }

  @Test
  public void testBuildQueryStringSpecialCharacter() {
    String rawQuery = "SELECT '{\"test ,:+\\\"inner\\\"/\\|?#><\"}'";
    String expectedQueryInParams = "SELECT '{\\\"test ,:+\\\\\\\"inner\\\\\\\"/\\\\|?#><\\\"}'";
    String params = SparkSubmitParameters.builder().query(rawQuery).build().toString();
    assertTrue(params.contains(expectedQueryInParams));
  }

  @Test
  public void testOverrideConfigItem() {
    SparkSubmitParameters params = SparkSubmitParameters.builder().build();
    params.setConfigItem(SPARK_JARS_KEY, "Overridden");
    String result = params.toString();

    assertTrue(result.contains(String.format("%s=Overridden", SPARK_JARS_KEY)));
  }

  @Test
  public void testDeleteConfigItem() {
    SparkSubmitParameters params = SparkSubmitParameters.builder().build();
    params.deleteConfigItem(HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY);
    String result = params.toString();

    assertFalse(result.contains(HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY));
  }

  @Test
  public void testAddConfigItem() {
    SparkSubmitParameters params = SparkSubmitParameters.builder().build();
    params.setConfigItem("AdditionalKey", "Value");
    String result = params.toString();

    assertTrue(result.contains("AdditionalKey=Value"));
  }
}

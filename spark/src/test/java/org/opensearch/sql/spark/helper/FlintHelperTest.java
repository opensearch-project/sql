package org.opensearch.sql.spark.helper;

import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_AUTH;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_HOST;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_PORT;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_REGION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_SCHEME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INTEGRATION_JAR;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FlintHelperTest {

  private static final String JAR = "JAR";
  private static final String HOST = "HOST";
  private static final String PORT = "PORT";
  private static final String SCHEME = "SCHEME";
  private static final String AUTH = "AUTH";
  private static final String REGION = "REGION";

  @Test
  public void testConstructorWithNull() {
    FlintHelper helper = new FlintHelper(null, null, null, null, null, null);

    Assertions.assertEquals(FLINT_INTEGRATION_JAR, helper.getFlintIntegrationJar());
    Assertions.assertEquals(FLINT_DEFAULT_HOST, helper.getFlintHost());
    Assertions.assertEquals(FLINT_DEFAULT_PORT, helper.getFlintPort());
    Assertions.assertEquals(FLINT_DEFAULT_SCHEME, helper.getFlintScheme());
    Assertions.assertEquals(FLINT_DEFAULT_AUTH, helper.getFlintAuth());
    Assertions.assertEquals(FLINT_DEFAULT_REGION, helper.getFlintRegion());
  }

  @Test
  public void testConstructor() {
    FlintHelper helper = new FlintHelper(JAR, HOST, PORT, SCHEME, AUTH, REGION);

    Assertions.assertEquals(JAR, helper.getFlintIntegrationJar());
    Assertions.assertEquals(HOST, helper.getFlintHost());
    Assertions.assertEquals(PORT, helper.getFlintPort());
    Assertions.assertEquals(SCHEME, helper.getFlintScheme());
    Assertions.assertEquals(AUTH, helper.getFlintAuth());
    Assertions.assertEquals(REGION, helper.getFlintRegion());
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.helper;

import lombok.Getter;

public class FlintHelper {
  // TODO should be replaced with mvn jar.
  public static final String FLINT_INTEGRATION_JAR =
      "s3://spark-datasource/flint-spark-integration-assembly-0.3.0-SNAPSHOT.jar";
  public static final String FLINT_DEFAULT_HOST = "localhost";
  public static final String FLINT_DEFAULT_PORT = "9200";
  public static final String FLINT_DEFAULT_SCHEME = "http";
  public static final String FLINT_DEFAULT_AUTH = "noauth";
  public static final String FLINT_DEFAULT_REGION = "us-west-2";

  @Getter private final String flintIntegrationJar;
  @Getter private final String flintHost;
  @Getter private final String flintPort;
  @Getter private final String flintScheme;
  @Getter private final String flintAuth;
  @Getter private final String flintRegion;

  /**
   * Arguments required to write data to opensearch index using flint integration.
   *
   * @param flintHost Opensearch host for flint
   * @param flintPort Opensearch port for flint integration
   * @param flintScheme Opensearch scheme for flint integration
   * @param flintAuth Opensearch auth for flint integration
   * @param flintRegion Opensearch region for flint integration
   */
  public FlintHelper(
      String flintIntegrationJar,
      String flintHost,
      String flintPort,
      String flintScheme,
      String flintAuth,
      String flintRegion) {
    this.flintIntegrationJar =
        flintIntegrationJar == null ? FLINT_INTEGRATION_JAR : flintIntegrationJar;
    this.flintHost = flintHost != null ? flintHost : FLINT_DEFAULT_HOST;
    this.flintPort = flintPort != null ? flintPort : FLINT_DEFAULT_PORT;
    this.flintScheme = flintScheme != null ? flintScheme : FLINT_DEFAULT_SCHEME;
    this.flintAuth = flintAuth != null ? flintAuth : FLINT_DEFAULT_AUTH;
    this.flintRegion = flintRegion != null ? flintRegion : FLINT_DEFAULT_REGION;
  }
}

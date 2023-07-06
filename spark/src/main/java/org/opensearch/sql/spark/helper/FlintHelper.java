/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.helper;

import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_AUTH;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_HOST;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_PORT;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_REGION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_SCHEME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INTEGRATION_JAR;

import lombok.Getter;

public class FlintHelper {
  @Getter
  private final String flintIntegrationJar;
  @Getter
  private final String flintHost;
  @Getter
  private final String flintPort;
  @Getter
  private final String flintScheme;
  @Getter
  private final String flintAuth;
  @Getter
  private final String flintRegion;

  /** Arguments required to write data to opensearch index using flint integration.
   *
   * @param flintHost   Opensearch host for flint
   * @param flintPort   Opensearch port for flint integration
   * @param flintScheme Opensearch scheme for flint integration
   * @param flintAuth   Opensearch auth for flint integration
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

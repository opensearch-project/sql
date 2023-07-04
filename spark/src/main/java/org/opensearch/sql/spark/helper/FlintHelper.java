package org.opensearch.sql.spark.helper;

import lombok.Getter;

import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_AUTH;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_HOST;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_PORT;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_REGION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_SCHEME;

public class FlintHelper {
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

  public FlintHelper(String flintHost, String flintPort, String flintScheme, String flintAuth, String flintRegion) {
    this.flintHost = flintHost != null ? flintHost : FLINT_DEFAULT_HOST;
    this.flintPort = flintPort != null ? flintPort : FLINT_DEFAULT_PORT;
    this.flintScheme = flintScheme != null ? flintScheme : FLINT_DEFAULT_SCHEME;
    this.flintAuth = flintAuth != null ? flintAuth : FLINT_DEFAULT_AUTH;
    this.flintRegion = flintRegion != null ? flintRegion : FLINT_DEFAULT_REGION;
  }
}

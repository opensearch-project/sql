/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

/** {@link CrossClusterRemoteSchemaCoverageIT} on the Calcite engine. */
public class CalciteCrossClusterRemoteSchemaCoverageIT extends CrossClusterRemoteSchemaCoverageIT {

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
  }
}

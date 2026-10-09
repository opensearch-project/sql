/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

/** {@link CrossClusterRemoteSchemaIT} on the Calcite engine. */
public class CalciteCrossClusterRemoteSchemaIT extends CrossClusterRemoteSchemaIT {

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
  }
}

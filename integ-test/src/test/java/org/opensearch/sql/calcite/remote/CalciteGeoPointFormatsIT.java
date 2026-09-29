/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.Capability.GEOPOINT_TYPE;

import org.opensearch.sql.ppl.GeoPointFormatsIT;
import org.opensearch.sql.util.RequiresCapability;

@RequiresCapability(GEOPOINT_TYPE)
public class CalciteGeoPointFormatsIT extends GeoPointFormatsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }
}

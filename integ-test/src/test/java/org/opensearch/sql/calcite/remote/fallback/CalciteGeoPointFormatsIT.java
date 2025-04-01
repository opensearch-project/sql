/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.fallback;

import org.junit.Ignore;
import org.opensearch.sql.ppl.GeoPointFormatsIT;

public class CalciteGeoPointFormatsIT extends GeoPointFormatsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Ignore("Need to support metadata, https://github.com/opensearch-project/sql/issues/3333")
  @Override
  public void testReadingGeoHash() {}
}

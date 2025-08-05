/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.opensearch.sql.ppl.GeoPointFormatsIT;

public class CalciteGeoPointFormatsIT extends GeoPointFormatsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Override
  public void testReadingGeoHash() throws IOException {
    withFallbackEnabled(
        () -> {
          try {
            super.testReadingGeoHash();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "Need to support metadata, https://github.com/opensearch-project/sql/issues/3333");
  }
}

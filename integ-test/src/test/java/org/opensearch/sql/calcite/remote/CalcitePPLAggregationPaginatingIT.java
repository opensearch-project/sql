/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.junit.After;

public class CalcitePPLAggregationPaginatingIT extends CalcitePPLAggregationIT {

  @Override
  public void init() throws Exception {
    super.init();
    setQueryBucketSize(2);
  }

  @After
  public void tearDown() throws Exception {
    resetQueryBucketSize();
    super.tearDown();
  }
}

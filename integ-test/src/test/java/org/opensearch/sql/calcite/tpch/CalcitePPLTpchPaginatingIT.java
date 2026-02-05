/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.tpch;

import org.junit.After;
import org.junit.Ignore;
import org.opensearch.sql.util.Retry;

@Ignore
@Retry
public class CalcitePPLTpchPaginatingIT extends CalcitePPLTpchIT {

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

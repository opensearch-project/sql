/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.ppl.DescribeCommandIT;

@Ignore("https://github.com/opensearch-project/sql/issues/3460")
public class NonFallbackCalciteDescribeCommandIT extends DescribeCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}

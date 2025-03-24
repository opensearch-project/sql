/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteTextFunctionIT;

public class NonFallbackCalciteTextFunctionIT extends CalciteTextFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3467")
  @Override
  public void testAscii() {}

  @Ignore("https://github.com/opensearch-project/sql/issues/3467")
  @Override
  public void testLeft() {}

  @Ignore("https://github.com/opensearch-project/sql/issues/3467")
  @Override
  public void testLocate() {}

  @Ignore("https://github.com/opensearch-project/sql/issues/3467")
  @Override
  public void testReplace() {}

  @Ignore("https://github.com/opensearch-project/sql/issues/3467")
  @Override
  public void testStrcmp() {}

  @Ignore("https://github.com/opensearch-project/sql/issues/3467")
  @Override
  public void testSubstr() {}
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.fallback;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.IPFunctionsIT;

public class CalciteIPFunctionsIT extends IPFunctionsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3505")
  @Override
  public void test_cidrmatch() throws IOException {}
  ;
}

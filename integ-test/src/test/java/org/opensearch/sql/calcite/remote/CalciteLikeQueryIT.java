/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Assume;
import org.junit.Test;
import org.opensearch.sql.ppl.LikeQueryIT;

public class CalciteLikeQueryIT extends LikeQueryIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  @Test
  public void test_convert_field_text_to_keyword() throws IOException {
    Assume.assumeTrue("Pushdown is not enabled, skipping this test.", isPushdownEnabled());
    super.test_convert_field_text_to_keyword();
  }
}

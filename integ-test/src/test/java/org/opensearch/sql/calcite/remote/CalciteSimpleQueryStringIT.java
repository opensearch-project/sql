/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.SimpleQueryStringIT;

public class CalciteSimpleQueryStringIT extends SimpleQueryStringIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    // Search Functions are not supported
    // TODO: "https://github.com/opensearch-project/sql/issues/3462"
    // disallowCalciteFallback();
  }
}

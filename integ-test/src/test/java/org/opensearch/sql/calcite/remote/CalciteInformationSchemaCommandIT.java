/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.junit.Ignore;
import org.opensearch.sql.ppl.InformationSchemaCommandIT;

@Ignore("https://github.com/opensearch-project/sql/issues/3455")
public class CalciteInformationSchemaCommandIT extends InformationSchemaCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    // TODO: "https://github.com/opensearch-project/sql/issues/3455"
    // disallowCalciteFallback();
  }
}

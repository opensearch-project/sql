/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.WhereCommandIT;

public class CalciteWhereCommandIT extends WhereCommandIT {
  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  @Override
  public void testIsNotNullFunction() throws IOException {}
  ;

  @Ignore("https://github.com/opensearch-project/sql/issues/3333")
  @Override
  public void testWhereWithMetadataFields() throws IOException {}
  ;
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.SortCommandIT;

/**
 * TODO there seems a bug in Calcite planner with sort. Fix {@link
 * org.opensearch.sql.calcite.standalone.CalcitePPLSortIT} first. then enable this IT and remove
 * this java doc.
 */
@Ignore
public class CalciteSortCommandIT extends SortCommandIT {
  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }
}

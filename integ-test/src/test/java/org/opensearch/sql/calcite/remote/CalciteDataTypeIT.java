/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;

import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.DataTypeIT;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

public class CalciteDataTypeIT extends DataTypeIT {

  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }

  @Override
  @Test
  @Ignore("ignore this class since IP type is unsupported in calcite engine")
  public void test_nonnumeric_data_types() throws IOException {
    super.test_nonnumeric_data_types();
  }
}

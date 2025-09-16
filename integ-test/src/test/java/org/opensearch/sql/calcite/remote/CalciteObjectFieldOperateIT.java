/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DEEP_NESTED;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.ObjectFieldOperateIT;

public class CalciteObjectFieldOperateIT extends ObjectFieldOperateIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields city | sort city.name ", TEST_INDEX_DEEP_NESTED));
    verifySchema(result, schema("city", "struct"));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

class IndexDMLHandlerTest {
  @Test
  public void getResponseFromExecutor() {
    JSONObject result =
        new IndexDMLHandler(null, null, null, null, null).getResponseFromExecutor(null);

    assertEquals("running", result.getString(STATUS_FIELD));
    assertEquals("", result.getString(ERROR_FIELD));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

public class OpenSearchStateStoreUtilTest {

  @Test
  void getIndexName() {
    String result = OpenSearchStateStoreUtil.getIndexName("DATASOURCE");

    assertEquals(".query_execution_request_datasource", result);
  }
}

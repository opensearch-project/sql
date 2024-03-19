/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.spark.client.StartJobRequest.DEFAULT_JOB_TIMEOUT;

import java.util.Map;
import org.junit.jupiter.api.Test;

class StartJobRequestTest {

  @Test
  void executionTimeout() {
    assertEquals(DEFAULT_JOB_TIMEOUT, onDemandJob().executionTimeout());
    assertEquals(0L, streamingJob().executionTimeout());
  }

  private StartJobRequest onDemandJob() {
    return new StartJobRequest("", "", "", "", Map.of(), false, null);
  }

  private StartJobRequest streamingJob() {
    return new StartJobRequest("", "", "", "", Map.of(), true, null);
  }
}

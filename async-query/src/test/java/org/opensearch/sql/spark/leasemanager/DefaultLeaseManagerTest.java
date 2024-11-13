/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.leasemanager;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.leasemanager.model.LeaseRequest;

@ExtendWith(MockitoExtension.class)
class DefaultLeaseManagerTest {
  @Mock private Settings settings;

  @Mock private StateStore stateStore;

  @Test
  public void concurrentSessionRuleOnlyApplyToInteractiveQuery() {
    assertTrue(
        new DefaultLeaseManager.ConcurrentSessionRule(settings, stateStore)
            .test(new LeaseRequest(JobType.BATCH, "mys3")));
    assertTrue(
        new DefaultLeaseManager.ConcurrentSessionRule(settings, stateStore)
            .test(new LeaseRequest(JobType.STREAMING, "mys3")));
  }

  @Test
  public void concurrentRefreshRuleNotAppliedToInteractiveAndBatchQuery() {
    assertTrue(
        new DefaultLeaseManager.ConcurrentRefreshJobRule(settings, stateStore)
            .test(new LeaseRequest(JobType.INTERACTIVE, "mys3")));
    assertTrue(
        new DefaultLeaseManager.ConcurrentRefreshJobRule(settings, stateStore)
            .test(new LeaseRequest(JobType.BATCH, "mys3")));
  }
}

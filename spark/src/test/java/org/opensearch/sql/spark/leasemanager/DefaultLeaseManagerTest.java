/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.leasemanager;

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
    new DefaultLeaseManager(settings, stateStore).borrow(new LeaseRequest(JobType.BATCH, "mys3"));
  }
}

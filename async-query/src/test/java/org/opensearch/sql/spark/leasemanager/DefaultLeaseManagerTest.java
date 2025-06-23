/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.leasemanager;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

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
  public void leaseManagerRejectsJobs() {
    when(stateStore.count(any(), any())).thenReturn(3L);
    when(settings.getSettingValue(any())).thenReturn(3);
    DefaultLeaseManager defaultLeaseManager = new DefaultLeaseManager(settings, stateStore);

    defaultLeaseManager.borrow(getLeaseRequest(JobType.BATCH));
    assertThrows(
        ConcurrencyLimitExceededException.class,
        () -> defaultLeaseManager.borrow(getLeaseRequest(JobType.INTERACTIVE)));
    assertThrows(
        ConcurrencyLimitExceededException.class,
        () -> defaultLeaseManager.borrow(getLeaseRequest(JobType.STREAMING)));
    assertThrows(
        ConcurrencyLimitExceededException.class,
        () -> defaultLeaseManager.borrow(getLeaseRequest(JobType.REFRESH)));
  }

  @Test
  public void leaseManagerAcceptsJobs() {
    when(stateStore.count(any(), any())).thenReturn(2L);
    when(settings.getSettingValue(any())).thenReturn(3);
    DefaultLeaseManager defaultLeaseManager = new DefaultLeaseManager(settings, stateStore);

    defaultLeaseManager.borrow(getLeaseRequest(JobType.BATCH));
    defaultLeaseManager.borrow(getLeaseRequest(JobType.INTERACTIVE));
    defaultLeaseManager.borrow(getLeaseRequest(JobType.STREAMING));
    defaultLeaseManager.borrow(getLeaseRequest(JobType.REFRESH));
  }

  private LeaseRequest getLeaseRequest(JobType jobType) {
    return new LeaseRequest(jobType, "mys3");
  }
}

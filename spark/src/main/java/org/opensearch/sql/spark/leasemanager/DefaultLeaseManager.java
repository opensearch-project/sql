/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.leasemanager;

import static org.opensearch.sql.common.setting.Settings.Key.SPARK_EXECUTION_SESSION_LIMIT;
import static org.opensearch.sql.spark.execution.statestore.StateStore.ALL_DATASOURCE;
import static org.opensearch.sql.spark.execution.statestore.StateStore.activeSessionsCount;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.leasemanager.model.LeaseRequest;

/**
 * Default Lease Manager
 * <li>QueryHandler borrow lease before execute the query.
 * <li>LeaseManagerService check request against domain level concurrent limit.
 * <li>LeaseManagerService running on data node and check limit based on cluster settings.
 */
public class DefaultLeaseManager implements LeaseManager {

  private final List<Rule<LeaseRequest>> concurrentLimitRules;
  private final Settings settings;
  private final StateStore stateStore;

  public DefaultLeaseManager(Settings settings, StateStore stateStore) {
    this.settings = settings;
    this.stateStore = stateStore;
    this.concurrentLimitRules = Arrays.asList(new ConcurrentSessionRule());
  }

  @Override
  public void borrow(LeaseRequest request) {
    for (Rule<LeaseRequest> rule : concurrentLimitRules) {
      if (!rule.test(request)) {
        throw new ConcurrencyLimitExceededException(rule.description());
      }
    }
  }

  interface Rule<T> extends Predicate<T> {
    String description();
  }

  public class ConcurrentSessionRule implements Rule<LeaseRequest> {
    @Override
    public String description() {
      return String.format("domain concurrent active session can not exceed %d", sessionMaxLimit());
    }

    @Override
    public boolean test(LeaseRequest leaseRequest) {
      if (leaseRequest.getJobType() != JobType.INTERACTIVE) {
        return true;
      }
      return activeSessionsCount(stateStore, ALL_DATASOURCE).get() < sessionMaxLimit();
    }

    public int sessionMaxLimit() {
      return settings.getSettingValue(SPARK_EXECUTION_SESSION_LIMIT);
    }
  }
}

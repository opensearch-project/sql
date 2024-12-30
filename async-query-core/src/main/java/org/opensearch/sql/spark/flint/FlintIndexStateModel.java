/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Flint Index Model maintain the index state. */
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = false)
public class FlintIndexStateModel extends StateModel {
  private final FlintIndexState indexState;
  private final String accountId;
  private final String applicationId;
  private final String jobId;
  private final String latestId;
  private final String datasourceName;
  private final long lastUpdateTime;
  private final String error;

  public static FlintIndexStateModel copy(
      FlintIndexStateModel copy, ImmutableMap<String, Object> metadata) {
    return builder()
        .indexState(copy.indexState)
        .accountId(copy.accountId)
        .applicationId(copy.applicationId)
        .jobId(copy.jobId)
        .latestId(copy.latestId)
        .datasourceName(copy.datasourceName)
        .lastUpdateTime(copy.lastUpdateTime)
        .error(copy.error)
        .metadata(metadata)
        .build();
  }

  public static FlintIndexStateModel copyWithState(
      FlintIndexStateModel copy, FlintIndexState state, ImmutableMap<String, Object> metadata) {
    return builder()
        .indexState(state)
        .accountId(copy.accountId)
        .applicationId(copy.applicationId)
        .jobId(copy.jobId)
        .latestId(copy.latestId)
        .datasourceName(copy.datasourceName)
        .lastUpdateTime(copy.lastUpdateTime)
        .error(copy.error)
        .metadata(metadata)
        .build();
  }

  @Override
  public String getId() {
    return latestId;
  }
}

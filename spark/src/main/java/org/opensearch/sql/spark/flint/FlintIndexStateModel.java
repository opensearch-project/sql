/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Flint Index Model maintain the index state. */
@Getter
@Builder
@EqualsAndHashCode(callSuper = false)
public class FlintIndexStateModel extends StateModel {
  private final FlintIndexState indexState;
  private final String applicationId;
  private final String jobId;
  private final String latestId;
  private final String datasourceName;
  private final long lastUpdateTime;
  private final String error;

  @EqualsAndHashCode.Exclude private final long seqNo;
  @EqualsAndHashCode.Exclude private final long primaryTerm;

  public FlintIndexStateModel(
      FlintIndexState indexState,
      String applicationId,
      String jobId,
      String latestId,
      String datasourceName,
      long lastUpdateTime,
      String error,
      long seqNo,
      long primaryTerm) {
    this.indexState = indexState;
    this.applicationId = applicationId;
    this.jobId = jobId;
    this.latestId = latestId;
    this.datasourceName = datasourceName;
    this.lastUpdateTime = lastUpdateTime;
    this.error = error;
    this.seqNo = seqNo;
    this.primaryTerm = primaryTerm;
  }

  public static FlintIndexStateModel copy(FlintIndexStateModel copy, long seqNo, long primaryTerm) {
    return new FlintIndexStateModel(
        copy.indexState,
        copy.applicationId,
        copy.jobId,
        copy.latestId,
        copy.datasourceName,
        copy.lastUpdateTime,
        copy.error,
        seqNo,
        primaryTerm);
  }

  public static FlintIndexStateModel copyWithState(
      FlintIndexStateModel copy, FlintIndexState state, long seqNo, long primaryTerm) {
    return new FlintIndexStateModel(
        state,
        copy.applicationId,
        copy.jobId,
        copy.latestId,
        copy.datasourceName,
        copy.lastUpdateTime,
        copy.error,
        seqNo,
        primaryTerm);
  }

  @Override
  public String getId() {
    return latestId;
  }
}

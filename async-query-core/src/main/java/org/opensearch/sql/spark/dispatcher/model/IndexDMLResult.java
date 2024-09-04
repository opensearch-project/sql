/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Plugin create Index DML result. */
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = false)
public class IndexDMLResult extends StateModel {
  private final String queryId;
  private final String status;
  private final String error;
  private final String datasourceName;
  private final Long queryRunTime;
  private final Long updateTime;

  public static IndexDMLResult copy(IndexDMLResult copy, ImmutableMap<String, Object> metadata) {
    return builder()
        .queryId(copy.queryId)
        .status(copy.status)
        .error(copy.error)
        .datasourceName(copy.datasourceName)
        .queryRunTime(copy.queryRunTime)
        .updateTime(copy.updateTime)
        .metadata(metadata)
        .build();
  }

  @Override
  public String getId() {
    return queryId;
  }
}

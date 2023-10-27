/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import static org.opensearch.sql.spark.execution.session.SessionModel.DATASOURCE_NAME;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Plugin create Index DML result. */
@Data
@EqualsAndHashCode(callSuper = false)
public class IndexDMLResult extends StateModel {
  private static final String QUERY_ID = "queryId";
  private static final String QUERY_RUNTIME = "queryRunTime";
  private static final String UPDATE_TIME = "updateTime";
  private static final String DOC_ID_PREFIX = "index";

  private final String queryId;
  private final String status;
  private final String error;
  private final String datasourceName;
  private final Long queryRunTime;
  private final Long updateTime;

  public static IndexDMLResult copy(IndexDMLResult copy, long seqNo, long primaryTerm) {
    return new IndexDMLResult(
        copy.queryId,
        copy.status,
        copy.error,
        copy.datasourceName,
        copy.queryRunTime,
        copy.updateTime);
  }

  @Override
  public String getId() {
    return DOC_ID_PREFIX + queryId;
  }

  @Override
  public long getSeqNo() {
    return SequenceNumbers.UNASSIGNED_SEQ_NO;
  }

  @Override
  public long getPrimaryTerm() {
    return SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder
        .startObject()
        .field(QUERY_ID, queryId)
        .field("status", status)
        .field("error", error)
        .field(DATASOURCE_NAME, datasourceName)
        .field(QUERY_RUNTIME, queryRunTime)
        .field(UPDATE_TIME, updateTime)
        .field("result", ImmutableList.of())
        .field("schema", ImmutableList.of())
        .endObject();
    return builder;
  }
}

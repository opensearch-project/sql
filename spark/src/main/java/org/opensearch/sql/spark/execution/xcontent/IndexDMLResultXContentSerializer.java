/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.opensearch.sql.spark.dispatcher.model.IndexDMLResult.QUERY_RUNTIME;
import static org.opensearch.sql.spark.dispatcher.model.IndexDMLResult.UPDATE_TIME;
import static org.opensearch.sql.spark.execution.session.SessionModel.DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.statement.StatementModel.QUERY_ID;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.dispatcher.model.IndexDMLResult;

public class IndexDMLResultXContentSerializer implements XContentSerializer<IndexDMLResult> {
  @Override
  public XContentBuilder toXContent(IndexDMLResult dmlResult, ToXContent.Params params)
      throws IOException {
    return XContentFactory.jsonBuilder()
        .startObject()
        .field(QUERY_ID, dmlResult.getQueryId())
        .field("status", dmlResult.getStatus())
        .field("error", dmlResult.getError())
        .field(DATASOURCE_NAME, dmlResult.getDatasourceName())
        .field(QUERY_RUNTIME, dmlResult.getQueryRunTime())
        .field(UPDATE_TIME, dmlResult.getUpdateTime())
        .field("result", ImmutableList.of())
        .field("schema", ImmutableList.of())
        .endObject();
  }

  @Override
  public IndexDMLResult fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
    throw new UnsupportedOperationException("IndexDMLResult to fromXContent Not supported");
  }
}

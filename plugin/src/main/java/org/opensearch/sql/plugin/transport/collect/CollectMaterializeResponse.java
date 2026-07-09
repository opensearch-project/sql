/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.collect;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/** Outcome of a background collect materialization, persisted to {@code .tasks}. */
public class CollectMaterializeResponse extends ActionResponse implements ToXContentObject {

  private final String indexName;
  private final boolean succeeded;

  public CollectMaterializeResponse(String indexName, boolean succeeded) {
    this.indexName = indexName;
    this.succeeded = succeeded;
  }

  public CollectMaterializeResponse(StreamInput in) throws IOException {
    super(in);
    this.indexName = in.readString();
    this.succeeded = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(indexName);
    out.writeBoolean(succeeded);
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject();
    builder.field("index", indexName);
    builder.field("succeeded", succeeded);
    builder.endObject();
    return builder;
  }
}

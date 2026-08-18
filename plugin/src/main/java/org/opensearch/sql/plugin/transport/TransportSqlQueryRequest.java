/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.rest.RestChannel;
import org.opensearch.tasks.Task;

/**
 * Request for {@link SqlQueryAction}. Dispatched only locally via {@code
 * NodeClient.executeLocally}, so the {@link #work} and {@link #channel} references are carried
 * in-process (never serialized). The {@link #query} text becomes the coordinator task description so
 * consumers can recover the original SQL from the task.
 */
public class TransportSqlQueryRequest extends ActionRequest {

  /** Truncate very long queries so the task description stays bounded. */
  static final int MAX_DESCRIPTION_LENGTH = 4096;

  private final String query;

  /** The SQL execution to run under the coordinator task. Transient — local dispatch only. */
  private final transient Consumer<RestChannel> work;

  /** The REST channel the execution writes its response to. Transient — local dispatch only. */
  private final transient RestChannel channel;

  public TransportSqlQueryRequest(String query, Consumer<RestChannel> work, RestChannel channel) {
    this.query = query == null ? "" : query;
    this.work = work;
    this.channel = channel;
  }

  public TransportSqlQueryRequest(StreamInput in) throws IOException {
    super(in);
    this.query = in.readString();
    this.work = null;
    this.channel = null;
  }

  public Consumer<RestChannel> getWork() {
    return work;
  }

  public RestChannel getChannel() {
    return channel;
  }

  @Override
  public Task createTask(
      long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
    return new SqlQueryTask(id, type, action, getDescription(), parentTaskId, headers);
  }

  @Override
  public String getDescription() {
    if (query.length() > MAX_DESCRIPTION_LENGTH) {
      return query.substring(0, MAX_DESCRIPTION_LENGTH);
    }
    return query;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(query);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response.format;

import lombok.Getter;
import org.opensearch.sql.executor.execution.CommandPlan;
import org.opensearch.sql.protocol.response.QueryResult;

/**
 * A simple response formatter which contains no data.
 * Supposed to use with {@link CommandPlan} only.
 */
public class CommandResponseFormatter extends JsonResponseFormatter<QueryResult> {

  public CommandResponseFormatter() {
    super(Style.PRETTY);
  }

  @Override
  protected Object buildJsonObject(QueryResult response) {
    return new NoQueryResponse();
  }

  @Override
  public String format(Throwable t) {
    return new JdbcResponseFormatter(Style.PRETTY).format(t);
  }

  @Getter
  public static class NoQueryResponse {
    // in case of failure an exception is thrown
    private final boolean succeeded = true;
  }
}

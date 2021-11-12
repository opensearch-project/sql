/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.builder;

import java.util.Objects;
import org.opensearch.client.RestClient;
import org.opensearch.sql.doctest.core.request.SqlRequest;
import org.opensearch.sql.doctest.core.response.SqlResponse;

/**
 * Query and explain request tuple.
 */
public class Requests {

  private final RestClient client;
  private final SqlRequest query;
  private final SqlRequest explain;

  public Requests(RestClient client, SqlRequest query) {
    this(client, query, SqlRequest.NONE);
  }

  public Requests(RestClient client, SqlRequest query, SqlRequest explain) {
    this.client = client;
    this.query = query;
    this.explain = explain;
  }

  public SqlRequest query() {
    return query;
  }

  public SqlResponse queryResponse() {
    Objects.requireNonNull(query, "Query request is required");
    return query.send(client);
  }

  public SqlRequest explain() {
    return explain;
  }

  public SqlResponse explainResponse() {
    if (explain == SqlRequest.NONE) {
      return SqlResponse.NONE;
    }
    return explain.send(client);
  }
}

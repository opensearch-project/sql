/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.adapter;

import com.google.common.base.Strings;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;

/**
 * The definition of QueryPlan of QueryAction which works as the adapter to the current QueryAction
 * framework.
 */
public class QueryPlanQueryAction extends QueryAction {
  private final QueryPlanRequestBuilder requestBuilder;

  public QueryPlanQueryAction(QueryPlanRequestBuilder requestBuilder) {
    super(null, null);
    this.requestBuilder = requestBuilder;
  }

  @Override
  public SqlElasticRequestBuilder explain() {
    return requestBuilder;
  }

  @Override
  public Optional<List<String>> getFieldNames() {
    List<String> fieldNames =
        ((QueryPlanRequestBuilder) requestBuilder)
            .outputColumns().stream()
                .map(
                    node ->
                        Strings.isNullOrEmpty(node.getAlias()) ? node.getName() : node.getAlias())
                .collect(Collectors.toList());
    return Optional.of(fieldNames);
  }
}

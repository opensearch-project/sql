/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

public class MultiMatchQuery extends MultiFieldQuery<MultiMatchQueryBuilder> {
  /**
   *  Default constructor for MultiMatch configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MultiMatchQuery() {
    super(FunctionParameterRepository.MultiMatchQueryBuildActions);
  }

  @Override
  protected MultiMatchQueryBuilder createBuilder(ImmutableMap<String, Float> fields, String query) {
    return QueryBuilders.multiMatchQuery(query).fields(fields);
  }

  @Override
  protected String getQueryName() {
    return MultiMatchQueryBuilder.NAME;
  }
}

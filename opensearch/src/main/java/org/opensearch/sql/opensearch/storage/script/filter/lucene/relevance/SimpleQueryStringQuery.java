/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.SimpleQueryStringBuilder;

public class SimpleQueryStringQuery extends MultiFieldQuery<SimpleQueryStringBuilder> {
  /**
   * Default constructor for SimpleQueryString configures how RelevanceQuery.build() handles named
   * arguments.
   */
  public SimpleQueryStringQuery() {
    super(FunctionParameterRepository.SimpleQueryStringQueryBuildActions);
  }

  @Override
  protected SimpleQueryStringBuilder createBuilder(
      ImmutableMap<String, Float> fields, String query) {
    return QueryBuilders.simpleQueryStringQuery(query).fields(fields);
  }

  @Override
  protected String getQueryName() {
    return SimpleQueryStringBuilder.NAME;
  }
}

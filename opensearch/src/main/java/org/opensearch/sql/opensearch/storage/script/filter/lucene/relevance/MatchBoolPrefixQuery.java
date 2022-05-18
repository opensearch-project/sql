/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Initializes MatchBoolPrefixQueryBuilder from a FunctionExpression.
 */
public class MatchBoolPrefixQuery
    extends RelevanceQuery<MatchBoolPrefixQueryBuilder> {

  @Override
  protected MatchBoolPrefixQueryBuilder createQueryBuilder(String field, String query) {
    return QueryBuilders.matchBoolPrefixQuery(field, query);
  }

  @Override
  protected Map<String, QueryBuilderStep<MatchBoolPrefixQueryBuilder>> buildActionMap() {
    return ImmutableMap.<String, QueryBuilderStep<MatchBoolPrefixQueryBuilder>>builder().build();
  }
}

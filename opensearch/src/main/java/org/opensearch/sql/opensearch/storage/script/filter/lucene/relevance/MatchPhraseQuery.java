/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.LuceneQuery;

/**
 * Lucene query that builds a match_phrase query.
 */
public class MatchPhraseQuery extends RelevanceQuery<MatchPhraseQueryBuilder> {
  /**
   *  Default constructor for MatchPhraseQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MatchPhraseQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MatchPhraseQueryBuilder>>builder()
        .put("boost", (b, v) -> b.boost(Float.parseFloat(v.stringValue())))
        .put("analyzer", (b, v) -> b.analyzer(v.stringValue()))
        .put("slop", (b, v) -> b.slop(Integer.parseInt(v.stringValue())))
        .put("zero_terms_query", (b, v) -> b.zeroTermsQuery(
            org.opensearch.index.search.MatchQuery.ZeroTermsQuery.valueOf(v.stringValue())))
        .build());
  }

  @Override
  protected MatchPhraseQueryBuilder createQueryBuilder(String field, String query) {
    return QueryBuilders.matchPhraseQuery(field, query);
  }
}

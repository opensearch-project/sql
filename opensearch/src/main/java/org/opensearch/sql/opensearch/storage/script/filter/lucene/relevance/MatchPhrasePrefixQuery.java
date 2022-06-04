package org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance;

import com.google.common.collect.ImmutableMap;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
    * Lucene query that builds a match_phrase_prefix query.
    */
public class MatchPhrasePrefixQuery  extends RelevanceQuery<MatchPhrasePrefixQueryBuilder> {
  /**
   *  Default constructor for MatchPhrasePrefixQuery configures how RelevanceQuery.build() handles
   * named arguments.
   */
  public MatchPhrasePrefixQuery() {
    super(ImmutableMap.<String, QueryBuilderStep<MatchPhrasePrefixQueryBuilder>>builder()
        .build());
  }

  @Override
  protected MatchPhrasePrefixQueryBuilder createQueryBuilder(String field, String query) {
    return QueryBuilders.matchPhrasePrefixQuery(field, query);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.script.StringUtils;

public class LikeQuery extends LuceneQuery {
  @Override
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
    String field = OpenSearchTextType.convertTextToKeyword(ref.getAttr(), ref.type());
    Expression expr = func.getArguments().get(1);
    ExprValue literalValue = expr.valueOf();
    return createBuilder(field, literalValue.stringValue());
  }

  /**
   * Though WildcardQueryBuilder is required, LikeQuery needed its own class as
   * it is not a relevance function which wildcard_query is. The arguments in
   * LIKE are of type ReferenceExpression while wildcard_query are of type
   * NamedArgumentExpression
   */
  protected WildcardQueryBuilder createBuilder(String field, String query) {
    String matchText = StringUtils.convertSqlWildcardToLucene(query);
    return QueryBuilders.wildcardQuery(field, matchText).caseInsensitive(true);
  }
}

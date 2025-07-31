/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.script.StringUtils;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder.ScriptQueryUnSupportedException;

public class LikeQuery extends LuceneQuery {
  @Override
  public QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    String field = OpenSearchTextType.convertTextToKeyword(fieldName, fieldType);
    return createBuilder(field, literal.stringValue());
  }

  /**
   * Though WildcardQueryBuilder is required, LikeQuery needed its own class as it is not a
   * relevance function which wildcard_query is. The arguments in LIKE are of type
   * ReferenceExpression while wildcard_query are of type NamedArgumentExpression
   */
  protected WildcardQueryBuilder createBuilder(String field, String query) {
    String matchText = StringUtils.convertSqlWildcardToLuceneSafe(query);
    return QueryBuilders.wildcardQuery(field, matchText).caseInsensitive(true);
  }

  /**
   * Verify if the function supports like/wildcard query. Prefer to run wildcard query for keyword
   * type field. For text type field, it doesn't support cross term match because OpenSearch
   * internally break text to multiple terms and apply wildcard matching one by one, which is not
   * same behavior with regular like function without pushdown.
   *
   * @param func function Input function expression
   * @return boolean
   */
  @Override
  public boolean canSupport(FunctionExpression func) {
    if (func.getArguments().size() == 2
        && (func.getArguments().get(0) instanceof ReferenceExpression)
        && (func.getArguments().get(1) instanceof LiteralExpression
            || literalExpressionWrappedByCast(func))) {
      ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
      // Only support keyword type field
      if (OpenSearchTextType.toKeywordSubField(ref.getRawPath(), ref.getType()) != null) {
        return true;
      } else {
        // Script pushdown is not supported for text type field
        throw new ScriptQueryUnSupportedException(
            "text field wildcard doesn't support script query");
      }
    }
    return false;
  }
}

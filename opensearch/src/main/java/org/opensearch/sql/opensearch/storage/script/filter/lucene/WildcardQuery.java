/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;

/**
 * Lucene query that builds wildcard query.
 */
public class WildcardQuery extends LuceneQuery {

  @Override
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    fieldName = convertTextToKeyword(fieldName, fieldType);
    String matchText = convertSqlWildcardToLucene(literal.stringValue());
    return QueryBuilders.wildcardQuery(fieldName, matchText);
  }

  private String convertSqlWildcardToLucene(String text) {
    return text.replace('%', '*')
               .replace('_', '?');
  }

}

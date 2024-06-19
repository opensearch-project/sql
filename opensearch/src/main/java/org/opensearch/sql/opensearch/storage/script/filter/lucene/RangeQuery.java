/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import lombok.RequiredArgsConstructor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;

/** Lucene query that builds range query for non-quality comparison. */
@RequiredArgsConstructor
public class RangeQuery extends LuceneQuery {

  public enum Comparison {
    LT,
    GT,
    LTE,
    GTE,
    BETWEEN
  }

  /** Comparison that range query build for. */
  private final Comparison comparison;

  @Override
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    Object value = value(literal);

    RangeQueryBuilder query = QueryBuilders.rangeQuery(fieldName);
    switch (comparison) {
      case LT:
        return query.lt(value);
      case GT:
        return query.gt(value);
      case LTE:
        return query.lte(value);
      case GTE:
        return query.gte(value);
      default:
        throw new IllegalStateException("Comparison is supported by range query: " + comparison);
    }
  }

  private Object value(ExprValue literal) {
    return literal.value();
  }
}

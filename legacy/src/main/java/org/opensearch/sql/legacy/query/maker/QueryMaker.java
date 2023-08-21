/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.maker;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.join.query.JoinQueryBuilders;
import org.opensearch.sql.legacy.domain.Condition;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.exception.SqlParseException;

public class QueryMaker extends Maker {

  /**
   * @param where
   * @return
   * @throws SqlParseException
   */
  public static BoolQueryBuilder explain(Where where) throws SqlParseException {
    return explain(where, true);
  }

  public static BoolQueryBuilder explain(Where where, boolean isQuery) throws SqlParseException {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    while (where.getWheres().size() == 1) {
      where = where.getWheres().getFirst();
    }
    new QueryMaker().explanWhere(boolQuery, where);
    if (isQuery) {
      return boolQuery;
    }
    return QueryBuilders.boolQuery().filter(boolQuery);
  }

  private QueryMaker() {
    super(true);
  }

  private void explanWhere(BoolQueryBuilder boolQuery, Where where) throws SqlParseException {
    if (where instanceof Condition) {
      addSubQuery(boolQuery, where, (QueryBuilder) make((Condition) where));
    } else {
      BoolQueryBuilder subQuery = QueryBuilders.boolQuery();
      addSubQuery(boolQuery, where, subQuery);
      for (Where subWhere : where.getWheres()) {
        explanWhere(subQuery, subWhere);
      }
    }
  }

  /**
   * @param boolQuery
   * @param where
   * @param subQuery
   */
  private void addSubQuery(BoolQueryBuilder boolQuery, Where where, QueryBuilder subQuery) {
    if (where instanceof Condition) {
      Condition condition = (Condition) where;

      if (condition.isNested()) {
        // bugfix #628
        if ("missing".equalsIgnoreCase(String.valueOf(condition.getValue()))
            && (condition.getOPERATOR() == Condition.OPERATOR.IS
                || condition.getOPERATOR() == Condition.OPERATOR.EQ)) {
          boolQuery.mustNot(
              QueryBuilders.nestedQuery(
                  condition.getNestedPath(),
                  QueryBuilders.boolQuery().mustNot(subQuery),
                  ScoreMode.None));
          return;
        }

        subQuery = QueryBuilders.nestedQuery(condition.getNestedPath(), subQuery, ScoreMode.None);
      } else if (condition.isChildren()) {
        subQuery =
            JoinQueryBuilders.hasChildQuery(condition.getChildType(), subQuery, ScoreMode.None);
      }
    }

    if (where.getConn() == Where.CONN.AND) {
      boolQuery.must(subQuery);
    } else {
      boolQuery.should(subQuery);
    }
  }
}

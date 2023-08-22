/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.core;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;

/** All parameters required by QueryPlanner */
public class QueryParams {

  /** Request builder for first table */
  private final TableInJoinRequestBuilder request1;

  /** Request builder for second table */
  private final TableInJoinRequestBuilder request2;

  /** Join type, ex. inner join, left join */
  private final SQLJoinTableSource.JoinType joinType;

  /**
   * Join conditions in ON clause grouped by OR. For example, "ON (a.name = b.id AND a.age = b.age)
   * OR a.location = b.address" => list: [ [ (a.name, b.id), (a.age, b.age) ], [ (a.location,
   * b.address) ] ]
   */
  private final List<List<Map.Entry<Field, Field>>> joinConditions;

  public QueryParams(
      TableInJoinRequestBuilder request1,
      TableInJoinRequestBuilder request2,
      SQLJoinTableSource.JoinType joinType,
      List<List<Map.Entry<Field, Field>>> t1ToT2FieldsComparison) {
    this.request1 = request1;
    this.request2 = request2;
    this.joinType = joinType;
    this.joinConditions = t1ToT2FieldsComparison;
  }

  public TableInJoinRequestBuilder firstRequest() {
    return request1;
  }

  public TableInJoinRequestBuilder secondRequest() {
    return request2;
  }

  public SQLJoinTableSource.JoinType joinType() {
    return joinType;
  }

  public List<List<Map.Entry<Field, Field>>> joinConditions() {
    return joinConditions;
  }

  @Override
  public String toString() {
    return "QueryParams{"
        + "request1="
        + request1
        + ", request2="
        + request2
        + ", joinType="
        + joinType
        + ", joinConditions="
        + joinConditions
        + '}';
  }
}

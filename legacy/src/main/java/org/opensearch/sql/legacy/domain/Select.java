/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import static com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import static org.opensearch.sql.legacy.domain.Field.STAR;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.parser.SubQueryExpression;

/**
 * sql select
 *
 * @author ansj
 */
public class Select extends Query {

  /** Using this functions will cause query to execute as aggregation. */
  private static final Set<String> AGGREGATE_FUNCTIONS =
      ImmutableSet.of(
          "SUM",
          "MAX",
          "MIN",
          "AVG",
          "TOPHITS",
          "COUNT",
          "STATS",
          "EXTENDED_STATS",
          "PERCENTILES",
          "SCRIPTED_METRIC");

  private List<Hint> hints = new ArrayList<>();
  private List<Field> fields = new ArrayList<>();
  private List<List<Field>> groupBys = new ArrayList<>();
  private Having having;
  private List<Order> orderBys = new ArrayList<>();
  private int offset;
  private Integer rowCount;
  private boolean containsSubQueries;
  private List<SubQueryExpression> subQueries;
  private boolean selectAll = false;
  private JoinType nestedJoinType = JoinType.COMMA;

  public boolean isQuery = false;
  public boolean isAggregate = false;

  public static final int DEFAULT_LIMIT = 200;

  public Select() {}

  public List<Field> getFields() {
    return fields;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public void setRowCount(Integer rowCount) {
    this.rowCount = rowCount;
  }

  public void addGroupBy(Field field) {
    List<Field> wrapper = new ArrayList<>();
    wrapper.add(field);
    addGroupBy(wrapper);
  }

  public void addGroupBy(List<Field> fields) {
    isAggregate = true;
    selectAll = false;
    this.groupBys.add(fields);
  }

  public List<List<Field>> getGroupBys() {
    return groupBys;
  }

  public Having getHaving() {
    return having;
  }

  public void setHaving(Having having) {
    this.having = having;
  }

  public List<Order> getOrderBys() {
    return orderBys;
  }

  public int getOffset() {
    return offset;
  }

  public Integer getRowCount() {
    return rowCount;
  }

  public void addOrderBy(String nestedPath, String name, String type, Field field) {
    if ("_score".equals(name)) {
      isQuery = true;
    }
    this.orderBys.add(new Order(nestedPath, name, type, field));
  }

  public void addField(Field field) {
    if (field == null) {
      return;
    }
    if (field == STAR && !isAggregate) {
      // Ignore GROUP BY since columns present in result are decided by column list in GROUP BY
      this.selectAll = true;
      return;
    }

    if (field instanceof MethodField
        && AGGREGATE_FUNCTIONS.contains(field.getName().toUpperCase())) {
      isAggregate = true;
    }

    fields.add(field);
  }

  public List<Hint> getHints() {
    return hints;
  }

  public JoinType getNestedJoinType() {
    return nestedJoinType;
  }

  public void setNestedJoinType(JoinType nestedJoinType) {
    this.nestedJoinType = nestedJoinType;
  }

  public void fillSubQueries() {
    subQueries = new ArrayList<>();
    Where where = this.getWhere();
    fillSubQueriesFromWhereRecursive(where);
  }

  private void fillSubQueriesFromWhereRecursive(Where where) {
    if (where == null) {
      return;
    }
    if (where instanceof Condition) {
      Condition condition = (Condition) where;
      if (condition.getValue() instanceof SubQueryExpression) {
        this.subQueries.add((SubQueryExpression) condition.getValue());
        this.containsSubQueries = true;
      }
      if (condition.getValue() instanceof Object[]) {

        for (Object o : (Object[]) condition.getValue()) {
          if (o instanceof SubQueryExpression) {
            this.subQueries.add((SubQueryExpression) o);
            this.containsSubQueries = true;
          }
        }
      }
    } else {
      for (Where innerWhere : where.getWheres()) {
        fillSubQueriesFromWhereRecursive(innerWhere);
      }
    }
  }

  public boolean containsSubQueries() {
    return containsSubQueries;
  }

  public List<SubQueryExpression> getSubQueries() {
    return subQueries;
  }

  public boolean isOrderdSelect() {
    return this.getOrderBys() != null && this.getOrderBys().size() > 0;
  }

  public boolean isSelectAll() {
    return selectAll;
  }
}

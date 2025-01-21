/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.opensearch.sql.calcite.utils.OpenSearchRelDataTypes;
import org.opensearch.sql.data.model.ExprValue;

public abstract class OpenSearchTable extends AbstractQueryableTable
    implements TranslatableTable, org.opensearch.sql.storage.Table {

  protected OpenSearchTable(Type elementType) {
    super(elementType);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return OpenSearchRelDataTypes.convertSchema(this);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    // return new LogicalTableScan(cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(), relOptTable);
    return new OpenSearchTableScan(cluster, relOptTable, this);
  }

  @Override
  public <T> Queryable<T> asQueryable(
      QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new OpenSearchQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override
  public Type getElementType() {
    return getRowType(null).getClass();
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  public abstract Enumerable<Object> search();
}

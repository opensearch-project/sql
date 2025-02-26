/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.lang.reflect.Type;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public abstract class OpenSearchTable extends AbstractQueryableTable
    implements TranslatableTable, org.opensearch.sql.storage.Table {

  protected OpenSearchTable(Type elementType) {
    super(elementType);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return OpenSearchTypeFactory.convertSchema(this);
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

/** not in use now */
public class OpenSearchQueryable<T> extends AbstractTableQueryable<T> {

  OpenSearchQueryable(
      QueryProvider queryProvider, SchemaPlus schema, OpenSearchTable table, String tableName) {
    super(queryProvider, schema, table, tableName);
  }

  @Override
  public Enumerator<T> enumerator() {
    //noinspection unchecked
    final Enumerable<T> enumerable = (Enumerable<T>) getTable().search();
    return enumerable.enumerator();
  }

  private OpenSearchTable getTable() {
    return (OpenSearchTable) table;
  }
}

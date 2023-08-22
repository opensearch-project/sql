/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.domain;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.domain.ColumnTypeProvider.COLUMN_DEFAULT_TYPE;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType;
import org.opensearch.sql.legacy.antlr.semantic.types.operator.SetOperator;
import org.opensearch.sql.legacy.antlr.semantic.types.special.Product;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.executor.format.Schema;

public class ColumnTypeProviderTest {
  @Test
  public void singleESDataTypeShouldReturnCorrectSchemaType() {
    assertEquals(Schema.Type.LONG, new ColumnTypeProvider(OpenSearchDataType.LONG).get(0));
  }

  @Test
  public void productTypeShouldReturnCorrectSchemaType() {
    ColumnTypeProvider columnTypeProvider =
        new ColumnTypeProvider(
            new Product(ImmutableList.of(OpenSearchDataType.LONG, OpenSearchDataType.SHORT)));
    assertEquals(Schema.Type.LONG, columnTypeProvider.get(0));
    assertEquals(Schema.Type.SHORT, columnTypeProvider.get(1));
  }

  @Test
  public void unSupportedTypeShouldReturnDefaultSchemaType() {
    ColumnTypeProvider columnTypeProvider = new ColumnTypeProvider(SetOperator.UNION);
    assertEquals(COLUMN_DEFAULT_TYPE, columnTypeProvider.get(0));
  }

  @Test
  public void providerWithoutColumnTypeShouldReturnDefaultSchemaType() {
    ColumnTypeProvider columnTypeProvider = new ColumnTypeProvider();
    assertEquals(COLUMN_DEFAULT_TYPE, columnTypeProvider.get(0));
  }
}

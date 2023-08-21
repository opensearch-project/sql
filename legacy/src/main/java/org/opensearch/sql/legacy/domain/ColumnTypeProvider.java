/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType;
import org.opensearch.sql.legacy.antlr.semantic.types.special.Product;
import org.opensearch.sql.legacy.executor.format.Schema;

/** The definition of column type provider */
public class ColumnTypeProvider {
  private final List<Schema.Type> typeList;

  private static final Map<OpenSearchDataType, Schema.Type> TYPE_MAP =
      new ImmutableMap.Builder<OpenSearchDataType, Schema.Type>()
          .put(OpenSearchDataType.SHORT, Schema.Type.SHORT)
          .put(OpenSearchDataType.LONG, Schema.Type.LONG)
          .put(OpenSearchDataType.INTEGER, Schema.Type.INTEGER)
          .put(OpenSearchDataType.FLOAT, Schema.Type.FLOAT)
          .put(OpenSearchDataType.DOUBLE, Schema.Type.DOUBLE)
          .put(OpenSearchDataType.KEYWORD, Schema.Type.KEYWORD)
          .put(OpenSearchDataType.TEXT, Schema.Type.TEXT)
          .put(OpenSearchDataType.STRING, Schema.Type.TEXT)
          .put(OpenSearchDataType.DATE, Schema.Type.DATE)
          .put(OpenSearchDataType.BOOLEAN, Schema.Type.BOOLEAN)
          .put(OpenSearchDataType.UNKNOWN, Schema.Type.DOUBLE)
          .build();
  public static final Schema.Type COLUMN_DEFAULT_TYPE = Schema.Type.DOUBLE;

  public ColumnTypeProvider(Type type) {
    this.typeList = convertOutputColumnType(type);
  }

  public ColumnTypeProvider() {
    this.typeList = new ArrayList<>();
  }

  /**
   * Get the type of column by index.
   *
   * @param index column index.
   * @return column type.
   */
  public Schema.Type get(int index) {
    if (typeList.isEmpty()) {
      return COLUMN_DEFAULT_TYPE;
    } else {
      return typeList.get(index);
    }
  }

  private List<Schema.Type> convertOutputColumnType(Type type) {
    if (type instanceof Product) {
      List<Type> types = ((Product) type).getTypes();
      return types.stream().map(t -> convertType(t)).collect(Collectors.toList());
    } else if (type instanceof OpenSearchDataType) {
      return ImmutableList.of(convertType(type));
    } else {
      return ImmutableList.of(COLUMN_DEFAULT_TYPE);
    }
  }

  private Schema.Type convertType(Type type) {
    try {
      return TYPE_MAP.getOrDefault(type, COLUMN_DEFAULT_TYPE);
    } catch (Exception e) {
      return COLUMN_DEFAULT_TYPE;
    }
  }
}

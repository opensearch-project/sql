/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.JsonBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/** An extension to {@link RelJson} to allow serialization & deserialization of UDTs */
public class ExtendedRelJson extends RelJson {
  private final JsonBuilder jsonBuilder;

  private ExtendedRelJson(JsonBuilder jsonBuilder) {
    super(jsonBuilder);
    this.jsonBuilder = jsonBuilder;
  }

  /** Creates a ExtendedRelJson. */
  public static ExtendedRelJson create(JsonBuilder jsonBuilder) {
    return new ExtendedRelJson(jsonBuilder);
  }

  @Override
  public @Nullable Object toJson(@Nullable Object value) {
    if (value instanceof RelDataTypeField) {
      return toJson((RelDataTypeField) value);
    } else if (value instanceof RelDataType) {
      return toJson((RelDataType) value);
    }
    return super.toJson(value);
  }

  private Object toJson(RelDataTypeField node) {
    final Map<String, @Nullable Object> map;
    if (node.getType().isStruct()) {
      map = jsonBuilder().map();
      map.put("fields", toJson(node.getType()));
      map.put("nullable", node.getType().isNullable());
    } else {
      //noinspection unchecked
      map = (Map<String, @Nullable Object>) toJson(node.getType());
    }
    map.put("name", node.getName());
    return map;
  }

  /** Modifies behavior for AbstractExprRelDataType instances, delegates to RelJson otherwise. */
  private Object toJson(RelDataType node) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    if (node.isStruct()) {
      final List<@Nullable Object> list = jsonBuilder().list();
      for (RelDataTypeField field : node.getFieldList()) {
        list.add(toJson(field));
      }
      map.put("fields", list);
      map.put("nullable", node.isNullable());
    } else {
      // For UDT like EXPR_TIMESTAMP, we additionally save its UDT info as a tag.
      if (node instanceof AbstractExprRelDataType) {
        map.put("udt", ((AbstractExprRelDataType<?>) node).getUdt().name());
      }
      map.put("type", node.getSqlTypeName().name());
      map.put("nullable", node.isNullable());
      if (node.getComponentType() != null) {
        map.put("component", toJson(node.getComponentType()));
      }
      RelDataType keyType = node.getKeyType();
      if (keyType != null) {
        map.put("key", toJson(keyType));
      }
      RelDataType valueType = node.getValueType();
      if (valueType != null) {
        map.put("value", toJson(valueType));
      }
      if (node.getSqlTypeName().allowsPrec()) {
        map.put("precision", node.getPrecision());
      }
      if (node.getSqlTypeName().allowsScale()) {
        map.put("scale", node.getScale());
      }
    }
    return map;
  }

  @Override
  public RelDataType toType(RelDataTypeFactory typeFactory, Object o) {
    if (o instanceof Map
        && ((Map<?, ?>) o).containsKey("udt")
        && typeFactory instanceof OpenSearchTypeFactory) {
      // Reconstruct UDT from its udt tag
      Object udtName = ((Map<?, ?>) o).get("udt");
      OpenSearchTypeFactory.ExprUDT udt = OpenSearchTypeFactory.ExprUDT.valueOf((String) udtName);
      return ((OpenSearchTypeFactory) typeFactory).createUDT(udt);
    }
    return super.toType(typeFactory, o);
  }

  private JsonBuilder jsonBuilder() {
    return requireNonNull(jsonBuilder, "jsonBuilder");
  }
}

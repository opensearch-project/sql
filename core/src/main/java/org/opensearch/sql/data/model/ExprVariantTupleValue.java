/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.calcite.runtime.rtti.BasicSqlTypeRtti;
import org.apache.calcite.runtime.rtti.RuntimeTypeInformation.RuntimeSqlTypeName;
import org.apache.calcite.runtime.variant.VariantValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.utils.ComparableLinkedHashMap;

/**
 * An {@link ExprTupleValue} that hands its values to Calcite as {@link VariantValue}s, so each
 * keeps the type it was written with instead of being flattened to one column type. Inside the
 * engine it is an ordinary tuple; the Calcite boundary is the only difference.
 *
 * <p>For a column whose type varies from row to row. OpenSearch's {@code flat_object} is the field
 * type that needs it today: its leaves have no mapping, but {@code _source} recorded each type.
 */
public class ExprVariantTupleValue extends ExprTupleValue {

  /** The rounding mode Calcite itself uses when it constructs a VARIANT from a value. */
  private static final RoundingMode ROUNDING_MODE =
      OpenSearchTypeFactory.TYPE_FACTORY.getTypeSystem().roundingMode();

  public ExprVariantTupleValue(LinkedHashMap<String, ExprValue> leaves) {
    super(leaves);
  }

  @Override
  public Object valueForCalcite() {
    ComparableLinkedHashMap<String, Object> result = new ComparableLinkedHashMap<>();
    for (Entry<String, ExprValue> entry : tupleValue().entrySet()) {
      result.put(entry.getKey(), toVariant(entry.getValue()));
    }
    return result;
  }

  /**
   * Wrap a leaf as the VARIANT runtime value Calcite expects, tagged with the leaf's type. A null
   * leaf is handed to Calcite as SQL NULL rather than as a variant null, so that nulls-first /
   * nulls-last ordering and IS NULL apply to it as to any other column.
   */
  static @Nullable VariantValue toVariant(ExprValue leaf) {
    if (leaf.isNull()) {
      return null;
    }
    if (leaf instanceof ExprCollectionValue array) {
      List<@Nullable VariantValue> elements = new ArrayList<>();
      for (ExprValue element : array.collectionValue()) {
        elements.add(toVariant(element));
      }
      return new ExprVariantValue.Array(elements);
    }
    return new ExprVariantValue(
        ROUNDING_MODE, leaf.valueForCalcite(), new BasicSqlTypeRtti(runtimeTypeOf(leaf.type())));
  }

  /**
   * The Java value inside a VARIANT. For a value this class produced it is held directly; for any
   * other variant, casting it to its own runtime type returns the wrapped value unchanged, and a
   * variant null casts to null.
   */
  public static @Nullable Object unwrap(VariantValue variant) {
    if (variant instanceof ExprVariantValue ours) {
      return ours.getValue();
    }
    if (variant instanceof ExprVariantValue.Array array) {
      return array.unwrapAll();
    }
    RuntimeSqlTypeName runtimeType = RuntimeSqlTypeName.valueOf(variant.getTypeString());
    if (runtimeType == RuntimeSqlTypeName.NULL) {
      return null;
    }
    return variant.cast(new BasicSqlTypeRtti(runtimeType));
  }

  private static RuntimeSqlTypeName runtimeTypeOf(ExprType type) {
    if (type instanceof ExprCoreType coreType) {
      switch (coreType) {
        case BYTE:
          return RuntimeSqlTypeName.TINYINT;
        case SHORT:
          return RuntimeSqlTypeName.SMALLINT;
        case INTEGER:
          return RuntimeSqlTypeName.INTEGER;
        case LONG:
          return RuntimeSqlTypeName.BIGINT;
        case FLOAT:
          return RuntimeSqlTypeName.REAL;
        case DOUBLE:
          return RuntimeSqlTypeName.DOUBLE;
        case BOOLEAN:
          return RuntimeSqlTypeName.BOOLEAN;
        case STRING:
          return RuntimeSqlTypeName.VARCHAR;
        default:
          break;
      }
    }
    throw new IllegalStateException("No variant runtime type for " + type.typeName());
  }
}

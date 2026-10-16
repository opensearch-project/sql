/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.type;

import java.util.Locale;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Element encoding of a {@link ExprCoreType#VECTOR} value, as declared by the {@code data_type}
 * parameter of an OpenSearch {@code knn_vector} mapping.
 *
 * <p>{@link #FLOAT} and {@link #HALF_FLOAT} vectors hold 32-bit float elements at runtime ({@code
 * java.lang.Float}); {@link #BYTE} and {@link #BINARY} vectors hold signed 8-bit elements ({@code
 * java.lang.Byte}). For {@link #BINARY} the mapping's {@code dimension} counts bits, so a value
 * holds {@code dimension / 8} bytes; see {@link #elementCount(int)}.
 */
public enum VectorDataType {
  /** 32-bit floats. The k-NN plugin's default when the mapping has no {@code data_type}. */
  FLOAT("float"),
  /** 16-bit floats, read and exposed as 32-bit floats. */
  HALF_FLOAT("half_float"),
  /** Signed 8-bit integers in [-128, 127]. */
  BYTE("byte"),
  /** Packed bits: each byte element holds eight dimensions. */
  BINARY("binary");

  /** The data type the k-NN plugin applies when a mapping does not declare one. */
  public static final VectorDataType DEFAULT = FLOAT;

  private final String mappingName;

  VectorDataType(String mappingName) {
    this.mappingName = mappingName;
  }

  /** The value of {@code data_type} in a {@code knn_vector} mapping, e.g. {@code half_float}. */
  public String getMappingName() {
    return mappingName;
  }

  /** Whether elements are 8-bit integers (byte, binary) rather than floats. */
  public boolean hasByteElements() {
    return this == BYTE || this == BINARY;
  }

  /**
   * Number of elements a value of this data type holds for a mapping dimension.
   *
   * @param dimension the {@code dimension} declared in the mapping, or a negative number if unknown
   * @return {@code dimension / 8} for {@link #BINARY}, {@code dimension} otherwise; the input
   *     unchanged if it is negative (unknown)
   */
  public int elementCount(int dimension) {
    if (dimension < 0) {
      return dimension;
    }
    return this == BINARY ? dimension / 8 : dimension;
  }

  /**
   * Resolve the {@code data_type} value of a {@code knn_vector} mapping.
   *
   * @param mappingName the declared value, case-insensitive; {@code null} means not declared
   * @return the matching data type; {@link #DEFAULT} if {@code mappingName} is {@code null}
   * @throws IllegalArgumentException if {@code mappingName} is not a known data type
   */
  public static VectorDataType fromMappingName(@Nullable String mappingName) {
    if (mappingName == null) {
      return DEFAULT;
    }
    String normalized = mappingName.toLowerCase(Locale.ROOT);
    for (VectorDataType type : values()) {
      if (type.mappingName.equals(normalized)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown knn_vector data_type [" + mappingName + "]");
  }
}

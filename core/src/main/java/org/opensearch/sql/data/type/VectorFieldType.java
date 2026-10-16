/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.type;

import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The type of a storage field that holds dense vectors, e.g. an OpenSearch {@code knn_vector}
 * field. Its core type is {@link ExprCoreType#VECTOR}; this interface adds the attributes a planner
 * needs and that a bare core type cannot carry.
 *
 * <p>{@code core} cannot depend on storage modules, so storage-specific types (for OpenSearch,
 * {@code OpenSearchKnnVectorType}) implement this interface and {@code
 * OpenSearchTypeFactory.convertExprTypeToRelDataType} turns it into the Calcite {@code
 * ExprVectorType} with the same dimension, data type and space type.
 */
public interface VectorFieldType extends ExprType {

  /** Dimension of a field whose mapping does not declare one, e.g. a model-based field. */
  int UNKNOWN_DIMENSION = -1;

  /**
   * The dimension as declared in the mapping. For {@link VectorDataType#BINARY} it counts bits.
   *
   * @return the dimension, or {@link #UNKNOWN_DIMENSION}
   */
  int getDimension();

  /** The element encoding; never null. */
  VectorDataType getDataType();

  /**
   * The similarity declared by the mapping ({@code space_type} at the field or inside {@code
   * method}), lower-case as written, e.g. {@code l2}, {@code cosinesimil}, {@code innerproduct}.
   *
   * @return the space type, or {@code null} if the mapping does not declare one; callers apply the
   *     k-NN plugin's default in that case
   */
  @Nullable String getSpaceType();

  /**
   * Whether the indices of a multi-index query map the field with a different dimension, data type
   * or space type, which a k-NN search over it reports as <i>vector mapping mismatch</i>.
   */
  default boolean hasMappingMismatch() {
    return false;
  }

  /** The indices whose mappings mismatch, sorted; empty if they agree or are not known. */
  default List<String> getMappingMismatchIndices() {
    return List.of();
  }

  /**
   * The data type a stored value from the given index was indexed with, which decides how a base64
   * vector is decoded.
   *
   * @param index the index the value comes from, or {@code null} if not known
   * @return the data type, or {@code null} if the indices of a multi-index query disagree on
   *     whether elements are bytes and {@code index} does not tell which applies
   */
  default @Nullable VectorDataType getStoredDataType(@Nullable String index) {
    return getDataType();
  }

  /**
   * Whether the given type is a vector: {@link ExprCoreType#VECTOR} or a {@link VectorFieldType}.
   */
  static boolean isVector(@Nullable ExprType type) {
    if (type == null) {
      return false;
    }
    ExprType original = type.getOriginalExprType();
    return original == ExprCoreType.VECTOR || original instanceof VectorFieldType;
  }
}

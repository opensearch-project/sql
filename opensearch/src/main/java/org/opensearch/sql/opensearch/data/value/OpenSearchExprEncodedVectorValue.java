/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.data.model.ExprCollectionValue;

/**
 * A knn_vector value that {@code _source} holds as a base64 string or as a single number instead of
 * an array, both of which the k-NN plugin accepts. SQL and the V2 engine read it as the empty array
 * they read it as before; {@link #valueForCalcite()} gives the stored value, which the Calcite scan
 * decodes with {@code ExprVectorType.toRuntimeValue}.
 */
public class OpenSearchExprEncodedVectorValue extends ExprCollectionValue {

  private final Object stored;

  /** The index of the hit the value comes from, when the scan needs it to decode the value. */
  private @Nullable String index;

  /**
   * @param stored the base64 string or the number
   */
  public OpenSearchExprEncodedVectorValue(Object stored) {
    super(List.of());
    this.stored = stored;
  }

  /** The base64 string or the number, as stored. */
  public Object getStored() {
    return stored;
  }

  /**
   * The index of the hit the value comes from, or {@code null} if not recorded. It is recorded when
   * the indices of a multi-index query disagree on whether the field's elements are bytes, so that
   * the value is decoded with its own index's data type.
   */
  public @Nullable String getIndex() {
    return index;
  }

  void setIndex(String index) {
    this.index = index;
  }

  @Override
  public Object valueForCalcite() {
    return stored;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.base;

import java.util.Objects;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;

/** Index type is not Enum because essentially each index is a brand new type. */
public class OpenSearchIndex implements BaseType {

  public enum IndexType {
    INDEX,
    NESTED_FIELD,
    INDEX_PATTERN
  }

  private final String indexName;
  private final IndexType indexType;

  public OpenSearchIndex(String indexName, IndexType indexType) {
    this.indexName = indexName;
    this.indexType = indexType;
  }

  public IndexType type() {
    return indexType;
  }

  @Override
  public String getName() {
    return indexName;
  }

  @Override
  public boolean isCompatible(Type other) {
    return equals(other);
  }

  @Override
  public String usage() {
    return indexType.name();
  }

  @Override
  public String toString() {
    return indexType + " [" + indexName + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OpenSearchIndex index = (OpenSearchIndex) o;
    return Objects.equals(indexName, index.indexName) && indexType == index.indexType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexName, indexType);
  }
}

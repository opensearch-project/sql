/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.opensearch.sql.data.type.ExprType;

/**
 * The type of alias. See <a
 * href="https://opensearch.org/docs/latest/opensearch/supported-field-types/alias/">doc</a>
 */
public class OpenSearchAliasType extends OpenSearchDataType {

  public static final String typeName = "alias";
  public static final String pathPropertyName = "path";
  public static final Set<MappingType> objectFieldTypes =
      Set.of(MappingType.Object, MappingType.Nested);
  private final String path;
  private final OpenSearchDataType originalType;

  public OpenSearchAliasType(String path, OpenSearchDataType type) {
    super(type.getExprCoreType());
    if (type instanceof OpenSearchAliasType) {
      throw new IllegalStateException(
          String.format("Alias field cannot refer to the path [%s] of alias type", path));
    } else if (objectFieldTypes.contains(type.getMappingType())) {
      throw new IllegalStateException(
          String.format("Alias field cannot refer to the path [%s] of object type", path));
    }
    this.path = path;
    this.originalType = type;
  }

  @Override
  public Optional<String> getOriginalPath() {
    return Optional.of(this.path);
  }

  @Override
  public ExprType getOriginalExprType() {
    return originalType.getExprType();
  }

  @Override
  public ExprType getExprType() {
    return this;
  }

  @Override
  public OpenSearchDataType cloneEmpty() {
    return new OpenSearchAliasType(this.path, originalType.cloneEmpty());
  }

  @Override
  public boolean isCompatible(ExprType other) {
    return originalType.isCompatible(other);
  }

  @Override
  public List<ExprType> getParent() {
    return originalType.getParent();
  }

  @Override
  public String typeName() {
    return originalType.typeName();
  }

  @Override
  public String legacyTypeName() {
    return originalType.legacyTypeName();
  }

  @Override
  public boolean shouldCast(ExprType other) {
    return originalType.shouldCast(other);
  }
}

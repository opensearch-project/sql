/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import lombok.EqualsAndHashCode;

/**
 * The type of a flat_object field. See <a
 * href="https://docs.opensearch.org/latest/mappings/supported-field-types/flat-object/">doc</a>
 *
 * <p>A flat_object declares no sub-fields in the index mapping: its keys exist only inside the
 * documents, and every leaf is indexed as a single keyword term with no numeric type. The engine
 * therefore reads its values from {@code _source} and presents the field as a map keyed by the
 * dotted leaf path, so that a nested object and a literal dotted key resolve to the same entry.
 *
 * <p>Like the other mapping types with no direct core-type counterpart (text, geo_point, binary),
 * it carries {@code UNKNOWN} as its core type so that {@link #getExprType()} returns this instance
 * and the Calcite type factory can map it by name.
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchFlatObjectType extends OpenSearchDataType {

  private static final OpenSearchFlatObjectType instance = new OpenSearchFlatObjectType();

  private OpenSearchFlatObjectType() {
    super(MappingType.FlatObject);
    exprCoreType = UNKNOWN;
  }

  public static OpenSearchFlatObjectType of() {
    return OpenSearchFlatObjectType.instance;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return instance;
  }
}

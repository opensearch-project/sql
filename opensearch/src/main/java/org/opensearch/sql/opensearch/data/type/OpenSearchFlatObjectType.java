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
 * <p>The core type below is the type of the field, which is that map -- not the type of a leaf,
 * which is text. Like the other mapping types with no core type that says so (text, geo_point,
 * binary), it is {@code UNKNOWN}, which is what makes {@link #getExprType()} return this instance
 * instead of a core type, so the Calcite type factory maps the field by name. Naming a core type
 * here would make the whole field that type: {@code STRING} would turn it into one string, with no
 * map to resolve a dotted path against.
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

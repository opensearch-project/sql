/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import lombok.EqualsAndHashCode;

/**
 * The type of a geo_point value. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/geo-point/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchGeoPointType extends OpenSearchDataType {

  private static final OpenSearchGeoPointType instance = new OpenSearchGeoPointType();

  private OpenSearchGeoPointType() {
    super(MappingType.GeoPoint);
    exprCoreType = UNKNOWN;
  }

  public static OpenSearchGeoPointType of() {
    return OpenSearchGeoPointType.instance;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return instance;
  }
}

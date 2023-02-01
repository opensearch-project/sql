/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * The type of a geo_point value. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/geo-point/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchGeoPointType extends OpenSearchDataType {

  @Getter
  private static final OpenSearchGeoPointType instance = new OpenSearchGeoPointType();

  private OpenSearchGeoPointType() {
    super(MappingType.GeoPoint);
    exprCoreType = UNKNOWN;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return instance;
  }
}

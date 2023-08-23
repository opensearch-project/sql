/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchGeoPointType;

/**
 * OpenSearch GeoPointValue.<br>
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
public class OpenSearchExprGeoPointValue extends ExprTupleValue {

  /**
   * Constructor for OpenSearchExprGeoPointValue.
   *
   * @param lat double value of latitude property of geo_point
   * @param lon double value of longitude property of geo_point
   */
  public OpenSearchExprGeoPointValue(Double lat, Double lon) {
    super(
        new LinkedHashMap<>(
            ImmutableMap.of(
                "lat", new ExprDoubleValue(lat),
                "lon", new ExprDoubleValue(lon))));
  }

  @Override
  public ExprType type() {
    return OpenSearchGeoPointType.of();
  }
}

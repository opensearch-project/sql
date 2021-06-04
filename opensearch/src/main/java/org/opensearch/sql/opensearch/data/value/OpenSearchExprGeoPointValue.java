/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_GEO_POINT;

import java.util.Objects;
import lombok.Data;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;

/**
 * OpenSearch GeoPointValue.
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
public class OpenSearchExprGeoPointValue extends AbstractExprValue {

  private final GeoPoint geoPoint;

  public OpenSearchExprGeoPointValue(Double lat, Double lon) {
    this.geoPoint = new GeoPoint(lat, lon);
  }

  @Override
  public Object value() {
    return geoPoint;
  }

  @Override
  public ExprType type() {
    return OPENSEARCH_GEO_POINT;
  }

  @Override
  public int compare(ExprValue other) {
    return geoPoint.toString()
        .compareTo((((OpenSearchExprGeoPointValue) other).geoPoint).toString());
  }

  @Override
  public boolean equal(ExprValue other) {
    return geoPoint.equals(((OpenSearchExprGeoPointValue) other).geoPoint);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(geoPoint);
  }

  @Data
  public static class GeoPoint {

    private final Double lat;

    private final Double lon;
  }
}

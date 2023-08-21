/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchGeoPointType;

class OpenSearchExprGeoPointValueTest {

  private OpenSearchExprGeoPointValue geoPointValue = new OpenSearchExprGeoPointValue(1.0, 1.0);

  @Test
  void value() {
    assertEquals(new OpenSearchExprGeoPointValue.GeoPoint(1.0, 1.0), geoPointValue.value());
  }

  @Test
  void type() {
    assertEquals(OpenSearchGeoPointType.of(), geoPointValue.type());
  }

  @Test
  void compare() {
    assertEquals(0, geoPointValue.compareTo(new OpenSearchExprGeoPointValue(1.0, 1.0)));
    assertEquals(geoPointValue, new OpenSearchExprGeoPointValue(1.0, 1.0));
  }

  @Test
  void equal() {
    assertTrue(geoPointValue.equal(new OpenSearchExprGeoPointValue(1.0, 1.0)));
  }

  @Test
  void testHashCode() {
    assertNotNull(geoPointValue.hashCode());
  }
}

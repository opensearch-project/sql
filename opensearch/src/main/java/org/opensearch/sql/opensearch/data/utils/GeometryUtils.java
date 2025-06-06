/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.utils;

import org.locationtech.jts.geom.GeometryFactory;

public interface GeometryUtils {
  GeometryFactory defaultFactory = new GeometryFactory();
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.spatial;

/** Created by Eliran on 15/8/2015. */
public class CellFilterParams {
  private final Point geohashPoint;
  private final int precision;
  private final boolean neighbors;

  public CellFilterParams(Point geohashPoint, int precision, boolean neighbors) {
    this.geohashPoint = geohashPoint;
    this.precision = precision;
    this.neighbors = neighbors;
  }

  public CellFilterParams(Point geohashPoint, int precision) {
    this(geohashPoint, precision, false);
  }

  public Point getGeohashPoint() {
    return geohashPoint;
  }

  public int getPrecision() {
    return precision;
  }

  public boolean isNeighbors() {
    return neighbors;
  }
}

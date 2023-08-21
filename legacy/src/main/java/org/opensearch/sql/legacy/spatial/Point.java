/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.spatial;

/** Created by Eliran on 1/8/2015. */
public class Point {
  private double lon;
  private double lat;

  public Point(double lon, double lat) {
    this.lon = lon;
    this.lat = lat;
  }

  public double getLon() {
    return lon;
  }

  public double getLat() {
    return lat;
  }
}

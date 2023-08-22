/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.spatial;

/** Created by Eliran on 1/8/2015. */
public class DistanceFilterParams {
  private String distance;
  private Point from;

  public DistanceFilterParams(String distance, Point from) {
    this.distance = distance;
    this.from = from;
  }

  public String getDistance() {
    return distance;
  }

  public Point getFrom() {
    return from;
  }
}

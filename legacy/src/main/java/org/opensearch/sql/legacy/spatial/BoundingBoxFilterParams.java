/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.spatial;

/** Created by Eliran on 1/8/2015. */
public class BoundingBoxFilterParams {
  private final Point topLeft;
  private final Point bottomRight;

  public BoundingBoxFilterParams(Point topLeft, Point bottomRight) {
    this.topLeft = topLeft;
    this.bottomRight = bottomRight;
  }

  public Point getTopLeft() {
    return topLeft;
  }

  public Point getBottomRight() {
    return bottomRight;
  }
}

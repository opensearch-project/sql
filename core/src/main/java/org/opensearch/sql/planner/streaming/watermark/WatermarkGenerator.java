/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

/**
 * A watermark generator generates watermark timestamp based on some strategy.
 */
public interface WatermarkGenerator {

  /**
   * Generate watermark timestamp on the given event timestamp.
   *
   * @param timestamp event timestamp.
   * @return watermark timestamp
   */
  long generate(long timestamp);

}

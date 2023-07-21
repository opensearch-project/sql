/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.streaming.watermark;

/**
 * A watermark generator generates watermark timestamp based on some strategy which is defined
 * in implementation class.
 */
public interface WatermarkGenerator {

  /**
   * Generate watermark timestamp on the given event timestamp.
   *
   * @param timestamp event timestamp in millisecond
   * @return watermark timestamp in millisecond
   */
  long generate(long timestamp);

}

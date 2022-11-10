/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

import org.opensearch.sql.data.model.ExprValue;

/**
 * A watermark generator generates watermark based on some strategy which is defined
 * in implementation class.
 */
public interface WatermarkGenerator {

  /**
   * Generate watermark value on the given value.
   *
   * @param value value
   * @return watermark
   */
  ExprValue generate(ExprValue value);

}

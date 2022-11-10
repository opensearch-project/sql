/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming;

import lombok.Data;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Stream context required by stream processing components and can be
 * stored and restored between executions.
 */
@Data
public class StreamContext {

  /** Current watermark value. */
  private ExprValue watermark;
}

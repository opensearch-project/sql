/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming;

import lombok.Data;

/**
 * Stream context required by stream processing components and can be stored and restored between
 * executions.
 */
@Data
public class StreamContext {

  /** Current watermark timestamp. */
  private long watermark;
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.streaming;

import lombok.Data;
import org.opensearch.sql.storage.split.Split;

/**
 * A batch of streaming execution.
 */
@Data
public class Batch {
  private final Split split;
}

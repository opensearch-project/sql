/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.streaming;

import lombok.Data;

/**
 * Offset.
 */
@Data
public class Offset {

  private final Long offset;
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.streaming;

import java.util.List;
import lombok.Data;
import org.opensearch.sql.storage.split.Split;

/**
 * A batch of streaming execution.
 */
@Data
public class Batch {
  private final List<Split> splits;
}

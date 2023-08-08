/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Optional;
import lombok.Getter;
import org.opensearch.sql.storage.split.Split;

/** Execution context hold planning related information. */
public class ExecutionContext {
  @Getter private final Optional<Split> split;

  public ExecutionContext(Split split) {
    this.split = Optional.of(split);
  }

  private ExecutionContext(Optional<Split> split) {
    this.split = split;
  }

  public static ExecutionContext emptyExecutionContext() {
    return new ExecutionContext(Optional.empty());
  }
}

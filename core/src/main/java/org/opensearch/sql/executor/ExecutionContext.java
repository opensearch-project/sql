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
  @Getter private final Integer querySizeLimit;

  public ExecutionContext(Split split) {
    this.split = Optional.of(split);
    this.querySizeLimit = null;
  }

  private ExecutionContext(Optional<Split> split, Integer querySizeLimit) {
    this.split = split;
    this.querySizeLimit = querySizeLimit;
  }

  public static ExecutionContext querySizeLimit(Integer querySizeLimit) {
    return new ExecutionContext(Optional.empty(), querySizeLimit);
  }

  public static ExecutionContext emptyExecutionContext() {
    return new ExecutionContext(Optional.empty(), null);
  }
}

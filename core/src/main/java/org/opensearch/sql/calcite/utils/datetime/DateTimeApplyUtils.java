/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.datetime;

import java.time.Duration;
import java.time.Instant;

public interface DateTimeApplyUtils {
  static Instant applyInterval(Instant base, Duration interval, boolean isAdd) {
    return isAdd ? base.plus(interval) : base.minus(interval);
  }
}

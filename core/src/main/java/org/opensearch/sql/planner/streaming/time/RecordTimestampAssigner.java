/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.time;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/**
 * Record timestamp assigner that use timestamp in record as event time.
 */
@RequiredArgsConstructor
public class RecordTimestampAssigner implements TimestampAssigner {

  private final Expression timestampField;

  @Override
  public long assign(ExprValue value) {
    ExprValue timestamp = timestampField.valueOf(value.bindingTuples());
    return timestamp.timestampValue().toEpochMilli();
  }
}

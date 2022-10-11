/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.streaming.event.Event;
import org.opensearch.sql.planner.streaming.event.RecordEvent;
import org.opensearch.sql.planner.streaming.time.TimestampAssigner;

/**
 * Watermark generator.
 */
@RequiredArgsConstructor
public abstract class WatermarkGenerator extends PhysicalPlan {

  private final PhysicalPlan input;

  private final TimestampAssigner timestampAssigner;

  private ExprValue next;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return null; // TODO
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return (next != null) || input.hasNext();
  }

  @Override
  public ExprValue next() {
    ExprValue result;
    if (next != null) {
      result = next;
      next = null;
    } else {
      next = input.next();

      long timestamp = timestampAssigner.assign(next);
      long watermark = onEvent(new RecordEvent(timestamp, next, null));
      result = new WatermarkEvent(watermark);
    }
    return result;
  }

  protected abstract long onEvent(Event event);
}

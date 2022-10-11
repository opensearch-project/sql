/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.collector.SpanCollector;
import org.opensearch.sql.planner.streaming.event.RecordEvent;
import org.opensearch.sql.planner.streaming.windowing.window.TimeWindow;
import org.opensearch.sql.planner.streaming.windowing.window.Window;

/**
 * Window state.
 */
@RequiredArgsConstructor
public class WindowAccumulator implements Iterable<Window> {

  private final SpanCollector collector;

  public void accumulate(RecordEvent event, Window window) {
    collector.collect(event.getData().bindingTuples());
  }

  public ExprValue get(Window window) {
    for (ExprValue bucket : collector.results()) {
      Map<String, ExprValue> data = bucket.tupleValue();
      if (data.get("span").timestampValue().toEpochMilli() == window.startTimestamp()) {
        return bucket;
      }
    }
    return null;
  }

  public void purge(Window window) {
    ExprDatetimeValue bucket = new ExprDatetimeValue(
        new ExprTimestampValue(
            Instant.ofEpochMilli(window.startTimestamp())).datetimeValue());
    collector.remove(bucket);
  }

  @Override
  public Iterator<Window> iterator() {
    return collector.results().stream()
        .map(value -> {
          long startTime = value.tupleValue().get("span").timestampValue().toEpochMilli();
          long endTime = startTime + collector.getSpanExpr().windowSize();
          return new TimeWindow(startTime, endTime);
        })
        .map(bucket -> (Window) bucket)
        .iterator();
  }
}

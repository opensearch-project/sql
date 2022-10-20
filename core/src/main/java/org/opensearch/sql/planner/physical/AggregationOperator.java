/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.planner.physical.collector.Collector;
import org.opensearch.sql.planner.physical.collector.SpanCollector;
import org.opensearch.sql.planner.streaming.WindowAccumulator;
import org.opensearch.sql.planner.streaming.event.RecordEvent;
import org.opensearch.sql.planner.streaming.event.StreamContext;
import org.opensearch.sql.planner.streaming.watermark.WatermarkEvent;
import org.opensearch.sql.planner.streaming.windowing.WindowingStrategy;
import org.opensearch.sql.planner.streaming.windowing.assigner.TumblingEventTimeWindowAssigner;
import org.opensearch.sql.planner.streaming.windowing.trigger.EventTimeWindowTrigger;
import org.opensearch.sql.planner.streaming.windowing.trigger.TriggerResult;
import org.opensearch.sql.planner.streaming.windowing.window.Window;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Group the all the input {@link BindingTuple} by {@link AggregationOperator#groupByExprList},
 * calculate the aggregation result by using {@link AggregationOperator#aggregatorList}.
 */
@EqualsAndHashCode
@ToString
public class AggregationOperator extends PhysicalPlan {

  private static final Logger LOG = LogManager.getLogger(AggregationOperator.class);

  @Getter
  private final PhysicalPlan input;
  @Getter
  private final List<NamedAggregator> aggregatorList;
  @Getter
  private final List<NamedExpression> groupByExprList;
  @Getter
  private final NamedExpression span;
  /**
   * {@link BindingTuple} Collector.
   */
  @EqualsAndHashCode.Exclude
  private Collector collector;

  @Getter
  private final StreamContext streamContext = new StreamContext();

  private WindowingStrategy windowingStrategy;

  private Iterator<ExprValue> nextRowBuffer = Collections.emptyIterator();

  /**
   * AggregationOperator Constructor.
   *
   * @param input           Input {@link PhysicalPlan}
   * @param aggregatorList  List of {@link Aggregator}
   * @param groupByExprList List of group by {@link Expression}
   */
  public AggregationOperator(PhysicalPlan input, List<NamedAggregator> aggregatorList,
                             List<NamedExpression> groupByExprList) {
    this.input = input;
    this.aggregatorList = aggregatorList;
    this.groupByExprList = groupByExprList;
    if (hasSpan(groupByExprList)) {
      // span expression is always the first expression in group list if exist.
      this.span = groupByExprList.get(0);
      this.collector =
          Collector.Builder.build(
              this.span, groupByExprList.subList(1, groupByExprList.size()), this.aggregatorList);
    } else {
      this.span = null;
      this.collector =
          Collector.Builder.build(this.span, this.groupByExprList, this.aggregatorList);
    }
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAggregation(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    if (!nextRowBuffer.hasNext()) {
      reloadBuffer();
    }
    return nextRowBuffer.hasNext();
  }

  @Override
  public ExprValue next() {
    return nextRowBuffer.next();
  }

  private void reloadBuffer() {
    do {
      // ??? For batch/micro-batch, input is bounded, so we fire all windows finally
      if (!input.hasNext()) {
        /*
        long latestWatermark = streamContext.getCurrentWatermark();
        nextRowBuffer = processWatermarkEvent(
            new WatermarkEvent(Long.MAX_VALUE)).iterator();
        streamContext.setCurrentWatermark(latestWatermark);
        */

        // Print out all remaining windows for demo purpose
        LOG.info("Window state to carry over to next micro batch: {}", collector);
        break;
      }

      // Continue processing if upstream is not exhausted
      ExprValue next = input.next();
      if (next instanceof WatermarkEvent) {
        nextRowBuffer = processWatermarkEvent((WatermarkEvent) next).iterator();
      } else {
        processRecordEvent(next);
      }
    } while (!nextRowBuffer.hasNext());
  }

  @Override
  public void open() {
    super.open();

    if (streamContext.getCollector() == null) {
      streamContext.setCollector(collector);
    } else {
      this.collector = streamContext.getCollector();
    }

    SpanExpression spanExpr = (SpanExpression) span.getDelegated();
    this.windowingStrategy = new WindowingStrategy(
        new TumblingEventTimeWindowAssigner(spanExpr.windowSize()),
        new WindowAccumulator((SpanCollector) collector),
        new EventTimeWindowTrigger(streamContext)
    );
  }

  private List<ExprValue> processWatermarkEvent(WatermarkEvent watermark) {
    streamContext.setCurrentWatermark(watermark.longValue());

    // Determine if trigger window pane emit or not
    List<Window> triggerWindows = new ArrayList<>();
    for (Window window : windowingStrategy.getWindowAccumulator()) {
      TriggerResult triggerResult = windowingStrategy.getWindowTrigger().onWindow(window);
      if (triggerResult.isFire()) {
        triggerWindows.add(window);
      }
    }

    // Collect and purge window (assume window should be purged once triggered)
    List<ExprValue> results = new ArrayList<>();
    for (Window window : triggerWindows) {
      results.add(windowingStrategy.getWindowAccumulator().get(window));
      windowingStrategy.getWindowAccumulator().purge(window);
    }

    if (!triggerWindows.isEmpty()) {
      LOG.info("Triggered window: {}", triggerWindows);
      LOG.info("Collected result: {}", results);
    }
    return results;
  }

  private void processRecordEvent(ExprValue next) {
    Expression timestampField = ((SpanExpression) span.getDelegated()).getField();
    long timestamp = timestampField.valueOf(next.bindingTuples()).timestampValue().toEpochMilli();

    // Assign window based on processing/event time
    //  and window type (tumbling, sliding, session, custom etc)
    RecordEvent event = new RecordEvent(timestamp, next, streamContext);
    Window window = windowingStrategy.getWindowAssigner().assign(event);

    // Discard late event if the window below watermark
    if (window.maxTimestamp() < streamContext.getCurrentWatermark()) {
      LOG.warn("Event is late and discarded: {}", next);
      return;
    }

    // Accumulate window state with the event data
    windowingStrategy.getWindowAccumulator().accumulate(event, window);
  }

  private boolean hasSpan(List<NamedExpression> namedExpressionList) {
    return !namedExpressionList.isEmpty()
        && namedExpressionList.get(0).getDelegated() instanceof SpanExpression;
  }
}

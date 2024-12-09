/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.WindowFunctionExpression;
import org.opensearch.sql.expression.window.frame.WindowFrame;

/** Physical operator for window function computation. */
@EqualsAndHashCode(callSuper = false)
@ToString
public class WindowOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;

  @Getter private final NamedExpression windowFunction;

  @Getter private final WindowDefinition windowDefinition;

  @EqualsAndHashCode.Exclude @ToString.Exclude private final WindowFrame windowFrame;

  /**
   * Peeking iterator that can peek next element which is required by window frame such as peer
   * frame to prefetch all rows related to same peer (of same sorting key).
   */
  @EqualsAndHashCode.Exclude @ToString.Exclude
  private final PeekingIterator<ExprValue> peekingIterator;

  /**
   * Initialize window operator.
   *
   * @param input child operator
   * @param windowFunction window function
   * @param windowDefinition window definition
   */
  public WindowOperator(
      PhysicalPlan input, NamedExpression windowFunction, WindowDefinition windowDefinition) {
    this.input = input;
    this.windowFunction = windowFunction;
    this.windowDefinition = windowDefinition;
    this.windowFrame = createWindowFrame();
    this.peekingIterator = Iterators.peekingIterator(input);
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitWindow(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of(input);
  }

  @Override
  public boolean hasNext() {
    return peekingIterator.hasNext() || windowFrame.hasNext();
  }

  @Override
  public ExprValue next() {
    windowFrame.load(peekingIterator);
    return enrichCurrentRowByWindowFunctionResult();
  }

  private WindowFrame createWindowFrame() {
    return ((WindowFunctionExpression) windowFunction.getDelegated())
        .createWindowFrame(windowDefinition);
  }

  private ExprValue enrichCurrentRowByWindowFunctionResult() {
    ImmutableMap.Builder<String, ExprValue> mapBuilder = new ImmutableMap.Builder<>();
    preserveAllOriginalColumns(mapBuilder);
    addWindowFunctionResultColumn(mapBuilder);
    return ExprTupleValue.fromExprValueMap(mapBuilder.build());
  }

  private void preserveAllOriginalColumns(ImmutableMap.Builder<String, ExprValue> mapBuilder) {
    ExprValue inputValue = windowFrame.current();
    inputValue.tupleValue().forEach(mapBuilder::put);
  }

  private void addWindowFunctionResultColumn(ImmutableMap.Builder<String, ExprValue> mapBuilder) {
    ExprValue exprValue = windowFunction.valueOf(windowFrame);
    mapBuilder.put(windowFunction.getName(), exprValue);
  }
}

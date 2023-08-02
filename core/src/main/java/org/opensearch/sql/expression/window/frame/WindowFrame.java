/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.frame;

import com.google.common.collect.PeekingIterator;
import java.util.Iterator;
import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;

/**
 * Window frame that represents a subset of a window which is all data accessible to
 * the window function when calculation. Basically there are 3 types of window frame:
 *  <ol>
 *  <li>Entire window frame that holds all data of the window</li>
 *  <li>Cumulative window frame that accumulates one row by another</li>
 *  <li>Sliding window frame that maintains a sliding window of fixed size</li>
 *  </ol>
 * Note that which type of window frame is used is determined by both window function itself
 * and frame definition in a window definition.
 */
public interface WindowFrame extends Environment<Expression, ExprValue>, Iterator<List<ExprValue>> {

  @Override
  default ExprValue resolve(Expression var) {
    return var.valueOf(current().bindingTuples());
  }

  /**
   * Check is current row the beginning of a new partition according to window definition.
   *
   * @return true if a new partition begins here, otherwise false.
   */
  boolean isNewPartition();

  /**
   * Load one or more rows as window function calculation needed.
   *
   * @param iterator peeking iterator that can peek next element without moving iterator
   */
  void load(PeekingIterator<ExprValue> iterator);

  /**
   * Get current data row for giving window operator chance to get rows preloaded into frame.
   *
   * @return data row
   */
  ExprValue current();
}

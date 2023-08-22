/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.frame;

import com.google.common.collect.PeekingIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.window.WindowDefinition;

/**
 * Window frame that only keep peers (tuples with same value of fields specified in sort list in
 * window definition). See PeerWindowFrameTest for details about how this window frame interacts
 * with window operator and window function.
 */
@RequiredArgsConstructor
public class PeerRowsWindowFrame implements WindowFrame {

  private final WindowDefinition windowDefinition;

  /**
   * All peer rows (peer means rows in a partition that share same sort key based on sort list in
   * window definition.
   */
  private final List<ExprValue> peers = new ArrayList<>();

  /** Which row in the peer is currently being enriched by window function. */
  private int position;

  /** Does row at current position represents a new partition. */
  private boolean isNewPartition = true;

  /** If any more pre-fetched rows not returned to window operator yet. */
  @Override
  public boolean hasNext() {
    return position < peers.size();
  }

  /**
   * Move position and clear new partition flag. Note that because all peer rows have same result
   * from window function, this is only returned at first time to change window function state.
   * Afterwards, empty list is returned to avoid changes until next peer loaded.
   *
   * @return all rows for the peer
   */
  @Override
  public List<ExprValue> next() {
    isNewPartition = false;
    if (position++ == 0) {
      return peers;
    }
    return Collections.emptyList();
  }

  /**
   * Current row at the position. Because rows are pre-fetched here, window operator needs to get
   * them from here too.
   *
   * @return row at current position that being enriched by window function
   */
  @Override
  public ExprValue current() {
    return peers.get(position);
  }

  /**
   * Preload all peer rows if last peer rows done. Note that when no more data in peeking iterator,
   * there must be rows in frame (hasNext()=true), so no need to check it.hasNext() in this method.
   * Load until: 1. Different peer found (row with different sort key) 2. Or new partition (row with
   * different partition key) 3. Or no more rows
   *
   * @param it rows iterator
   */
  @Override
  public void load(PeekingIterator<ExprValue> it) {
    if (hasNext()) {
      return;
    }

    // Reset state: reset new partition before clearing peers
    isNewPartition = !isSamePartition(it.peek());
    position = 0;
    peers.clear();

    while (it.hasNext()) {
      ExprValue next = it.peek();
      if (peers.isEmpty()) {
        peers.add(it.next());
      } else if (isSamePartition(next) && isPeer(next)) {
        peers.add(it.next());
      } else {
        break;
      }
    }
  }

  @Override
  public boolean isNewPartition() {
    return isNewPartition;
  }

  private boolean isPeer(ExprValue next) {
    List<Expression> sortFields =
        windowDefinition.getSortList().stream().map(Pair::getRight).collect(Collectors.toList());

    ExprValue last = peers.get(peers.size() - 1);
    return resolve(sortFields, last).equals(resolve(sortFields, next));
  }

  private boolean isSamePartition(ExprValue next) {
    if (peers.isEmpty()) {
      return false;
    }

    List<Expression> partitionByList = windowDefinition.getPartitionByList();
    ExprValue last = peers.get(peers.size() - 1);
    return resolve(partitionByList, last).equals(resolve(partitionByList, next));
  }

  private List<ExprValue> resolve(List<Expression> expressions, ExprValue row) {
    Environment<Expression, ExprValue> valueEnv = row.bindingTuples();
    return expressions.stream().map(expr -> expr.valueOf(valueEnv)).collect(Collectors.toList());
  }
}

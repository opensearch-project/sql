/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

public class JDBCUpdateCountResponseHandle implements JDBCResponseHandle {

  private final UnmodifiableIterator<ExprValue> iterator;

  public JDBCUpdateCountResponseHandle(int count) {
    iterator = Iterators.singletonIterator(integerValue(count));
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(
        ImmutableList.of(new ExecutionEngine.Schema.Column("result", "result", INTEGER)));
  }
}

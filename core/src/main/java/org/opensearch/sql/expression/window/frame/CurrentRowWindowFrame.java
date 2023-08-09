/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.frame;

import com.google.common.collect.PeekingIterator;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.window.WindowDefinition;

/**
 * Conceptually, cumulative window frame should hold all seen rows till next partition. This class
 * is actually an optimized version that only hold previous and current row. This is efficient and
 * sufficient for ranking and aggregate window function support for now, though need to add "real"
 * cumulative frame implementation in future as needed.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
@ToString
public class CurrentRowWindowFrame implements WindowFrame {

  @Getter private final WindowDefinition windowDefinition;

  private ExprValue previous;
  private ExprValue current;

  @Override
  public boolean isNewPartition() {
    Objects.requireNonNull(current);

    if (previous == null) {
      return true;
    }

    List<ExprValue> preValues = resolve(windowDefinition.getPartitionByList(), previous);
    List<ExprValue> curValues = resolve(windowDefinition.getPartitionByList(), current);
    return !preValues.equals(curValues);
  }

  @Override
  public void load(PeekingIterator<ExprValue> it) {
    previous = current;
    current = it.next();
  }

  @Override
  public ExprValue current() {
    return current;
  }

  public ExprValue previous() {
    return previous;
  }

  private List<ExprValue> resolve(List<Expression> expressions, ExprValue row) {
    Environment<Expression, ExprValue> valueEnv = row.bindingTuples();
    return expressions.stream().map(expr -> expr.valueOf(valueEnv)).collect(Collectors.toList());
  }

  /**
   * Current row window frame won't pre-fetch any row ahead. So always return false as nothing
   * "cached" in frame.
   */
  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public List<ExprValue> next() {
    return Collections.emptyList();
  }
}

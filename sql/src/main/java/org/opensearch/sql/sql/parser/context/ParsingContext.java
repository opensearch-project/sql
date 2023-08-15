/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.context;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * SQL parsing context that maintains stack of query specifications for nested queries. Currently
 * this is just a thin wrapper by a stack.
 */
public class ParsingContext {

  /**
   * Use stack rather than linked query specification because there is no need to look up through
   * the stack.
   */
  private final Deque<QuerySpecification> contexts = new ArrayDeque<>();

  public void push() {
    contexts.push(new QuerySpecification());
  }

  public QuerySpecification peek() {
    return contexts.peek();
  }

  /**
   * Pop up query context.
   *
   * @return query context after popup.
   */
  public QuerySpecification pop() {
    return contexts.pop();
  }
}

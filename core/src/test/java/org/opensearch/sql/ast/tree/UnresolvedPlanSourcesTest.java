/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.QualifiedName;

/**
 * Pins the {@link UnresolvedPlan#getSources()} contract: a node declares the plans it reads from
 * that {@code getChild()} does not expose, so consumers can find every source generically. A node
 * that drops its declaration here silently hides a source from them.
 */
class UnresolvedPlanSourcesTest {

  private static UnresolvedPlan relation(String name) {
    return new Relation(new QualifiedName(name));
  }

  @Test
  void defaultsToEmpty() {
    // A node whose sources are all reachable via getChild() declares none.
    assertTrue(relation("t").getSources().isEmpty());
  }

  @Test
  void appendDeclaresSubSearch() {
    UnresolvedPlan sub = relation("appended");
    assertEquals(List.of(sub), new Append(sub).attach(relation("base")).getSources());
  }

  @Test
  void appendColDeclaresSubSearch() {
    UnresolvedPlan sub = relation("appended");
    assertEquals(List.of(sub), new AppendCol(false, sub).attach(relation("base")).getSources());
  }

  @Test
  void appendPipeDeclaresSubQuery() {
    UnresolvedPlan sub = relation("appended");
    assertEquals(List.of(sub), new AppendPipe(sub).attach(relation("base")).getSources());
  }

  @Test
  void sourcesAreDisjointFromChild() {
    // The contract requires getSources() not to repeat getChild(), or consumers walking both would
    // visit the piped input twice.
    UnresolvedPlan child = relation("base");
    UnresolvedPlan sub = relation("appended");
    Append append = (Append) new Append(sub).attach(child);
    assertEquals(List.of(child), append.getChild());
    assertEquals(List.of(sub), append.getSources());
  }

  @Test
  void nullSourceYieldsEmptyRatherThanNpe() {
    // Nodes can be inspected before attach()/with an absent sub-search; the walk must not break.
    assertTrue(new AppendPipe(null).getSources().isEmpty());
    assertTrue(new Append(null).getSources().isEmpty());
    assertTrue(new AppendCol(false, null).getSources().isEmpty());
  }
}

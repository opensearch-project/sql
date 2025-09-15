/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

class MultisearchTest {

  @Test
  public void testMultisearchCreation() {
    // Create subsearches
    UnresolvedPlan subsearch1 = relation("table1");
    UnresolvedPlan subsearch2 =
        filter(relation("table2"), compare(">", field("age"), intLiteral(30)));
    List<UnresolvedPlan> subsearches = ImmutableList.of(subsearch1, subsearch2);

    // Create multisearch
    Multisearch multisearch = new Multisearch(subsearches);

    // Verify properties
    assertEquals(subsearches, multisearch.getSubsearches());
    assertEquals(2, multisearch.getSubsearches().size());
    assertNotNull(multisearch.getChild());
    assertEquals(subsearches, multisearch.getChild());
  }

  @Test
  public void testMultisearchWithChild() {
    UnresolvedPlan subsearch1 = relation("table1");
    UnresolvedPlan subsearch2 = relation("table2");
    List<UnresolvedPlan> subsearches = ImmutableList.of(subsearch1, subsearch2);

    Multisearch multisearch = new Multisearch(subsearches);
    UnresolvedPlan mainQuery = relation("main_table");

    // Attach child
    multisearch.attach(mainQuery);

    // Verify child is attached
    List<? extends Node> children = multisearch.getChild();
    assertEquals(3, children.size()); // main query + 2 subsearches
    assertEquals(mainQuery, children.get(0));
    assertEquals(subsearch1, children.get(1));
    assertEquals(subsearch2, children.get(2));
  }

  @Test
  public void testMultisearchVisitorAccept() {
    UnresolvedPlan subsearch = relation("table");
    Multisearch multisearch = new Multisearch(ImmutableList.of(subsearch));

    // Test visitor pattern
    TestVisitor visitor = new TestVisitor();
    String result = multisearch.accept(visitor, "test_context");

    assertEquals("visitMultisearch_called_with_test_context", result);
  }

  @Test
  public void testMultisearchEqualsAndHashCode() {
    UnresolvedPlan subsearch1 = relation("table1");
    UnresolvedPlan subsearch2 = relation("table2");
    List<UnresolvedPlan> subsearches = ImmutableList.of(subsearch1, subsearch2);

    Multisearch multisearch1 = new Multisearch(subsearches);
    Multisearch multisearch2 = new Multisearch(subsearches);

    assertEquals(multisearch1, multisearch2);
    assertEquals(multisearch1.hashCode(), multisearch2.hashCode());
  }

  @Test
  public void testMultisearchToString() {
    UnresolvedPlan subsearch = relation("table");
    Multisearch multisearch = new Multisearch(ImmutableList.of(subsearch));

    String toString = multisearch.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("Multisearch"));
  }

  @Test
  public void testMultisearchIsStreamingCommand() {
    UnresolvedPlan subsearch = relation("table");
    Multisearch multisearch = new Multisearch(ImmutableList.of(subsearch));

    // Multisearch should be a streaming command
    assertTrue(StreamingCommandClassifier.isStreamingCommand(multisearch));
  }

  // Test visitor implementation
  private static class TestVisitor extends AbstractNodeVisitor<String, String> {
    @Override
    public String visitMultisearch(Multisearch node, String context) {
      return "visitMultisearch_called_with_" + context;
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.planner.logical.LogicalMLCommons;
import org.opensearch.sql.planner.logical.LogicalPlan;

@ExtendWith(MockitoExtension.class)
public class OpenSearchDefaultImplementorTest {

  @Mock
  OpenSearchIndexScan indexScan;
  @Mock
  OpenSearchClient client;

  /**
   * For test coverage.
   */
  @Test
  public void visitInvalidTypeShouldThrowException() {
    final OpenSearchIndex.OpenSearchDefaultImplementor implementor =
        new OpenSearchIndex.OpenSearchDefaultImplementor(indexScan, client);

    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> implementor.visitNode(relation("index"),
            indexScan));
    ;
    assertEquals(
        "unexpected plan node type "
            + "class org.opensearch.sql.planner.logical.LogicalRelation",
        exception.getMessage());
  }

  @Test
  public void visitMachineLearning() {
    LogicalMLCommons node = Mockito.mock(LogicalMLCommons.class,
            Answers.RETURNS_DEEP_STUBS);
    Mockito.when(node.getChild().get(0)).thenReturn(Mockito.mock(LogicalPlan.class));
    OpenSearchIndex.OpenSearchDefaultImplementor implementor =
            new OpenSearchIndex.OpenSearchDefaultImplementor(indexScan, client);
    assertNotNull(implementor.visitMLCommons(node, indexScan));
  }
}

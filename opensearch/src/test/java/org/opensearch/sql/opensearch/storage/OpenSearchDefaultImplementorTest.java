/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAD;
import org.opensearch.sql.planner.logical.LogicalHighlight;
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

  @Test
  public void visitAD() {
    LogicalAD node = Mockito.mock(LogicalAD.class,
            Answers.RETURNS_DEEP_STUBS);
    Mockito.when(node.getChild().get(0)).thenReturn(Mockito.mock(LogicalPlan.class));
    OpenSearchIndex.OpenSearchDefaultImplementor implementor =
            new OpenSearchIndex.OpenSearchDefaultImplementor(indexScan, client);
    assertNotNull(implementor.visitAD(node, indexScan));
  }

  @Test
  public void visitHighlight() {
    LogicalHighlight node = Mockito.mock(LogicalHighlight.class,
        Answers.RETURNS_DEEP_STUBS);
    Mockito.when(node.getChild().get(0)).thenReturn(Mockito.mock(LogicalPlan.class));
    OpenSearchRequestBuilder requestBuilder = Mockito.mock(OpenSearchRequestBuilder.class);
    Mockito.when(indexScan.getRequestBuilder()).thenReturn(requestBuilder);
    OpenSearchIndex.OpenSearchDefaultImplementor implementor =
        new OpenSearchIndex.OpenSearchDefaultImplementor(indexScan, client);

    implementor.visitHighlight(node, indexScan);
    verify(requestBuilder).pushDownHighlight(any());
  }
}

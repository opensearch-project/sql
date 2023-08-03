/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.planner.logical.LogicalAD;
import org.opensearch.sql.planner.logical.LogicalML;
import org.opensearch.sql.planner.logical.LogicalMLCommons;
import org.opensearch.sql.planner.logical.LogicalPlan;

@ExtendWith(MockitoExtension.class)
public class OpenSearchDefaultImplementorTest {

  @Mock
  OpenSearchClient client;

  @Test
  public void visitMachineLearning() {
    LogicalMLCommons node = Mockito.mock(LogicalMLCommons.class,
        Answers.RETURNS_DEEP_STUBS);
    Mockito.when(node.getChild().get(0)).thenReturn(Mockito.mock(LogicalPlan.class));
    OpenSearchIndex.OpenSearchDefaultImplementor implementor =
        new OpenSearchIndex.OpenSearchDefaultImplementor(client);
    assertNotNull(implementor.visitMLCommons(node, null));
  }

  @Test
  public void visitAD() {
    LogicalAD node = Mockito.mock(LogicalAD.class,
        Answers.RETURNS_DEEP_STUBS);
    Mockito.when(node.getChild().get(0)).thenReturn(Mockito.mock(LogicalPlan.class));
    OpenSearchIndex.OpenSearchDefaultImplementor implementor =
        new OpenSearchIndex.OpenSearchDefaultImplementor(client);
    assertNotNull(implementor.visitAD(node, null));
  }

  @Test
  public void visitML() {
    LogicalML node = Mockito.mock(LogicalML.class,
            Answers.RETURNS_DEEP_STUBS);
    Mockito.when(node.getChild().get(0)).thenReturn(Mockito.mock(LogicalPlan.class));
    OpenSearchIndex.OpenSearchDefaultImplementor implementor =
            new OpenSearchIndex.OpenSearchDefaultImplementor(client);
    assertNotNull(implementor.visitML(node, null));
  }
}

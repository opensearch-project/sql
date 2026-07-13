/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ScriptDetectorTest {

  @Test
  void returnsFalseForNonScanNode() {
    RelNode mockNode = createMockNode();
    assertFalse(ScriptDetector.hasScripts(mockNode));
  }

  @Test
  void returnsTrueWhenScanHasScripts() {
    AbstractCalciteIndexScan scan = createMockScan(true);
    assertTrue(ScriptDetector.hasScripts(scan));
  }

  @Test
  void returnsFalseWhenScanHasNoScripts() {
    AbstractCalciteIndexScan scan = createMockScan(false);
    assertFalse(ScriptDetector.hasScripts(scan));
  }

  @Test
  void detectsScriptsInNestedPlan() {
    AbstractCalciteIndexScan scan = createMockScan(true);
    RelNode parent = createMockNode(scan);
    assertTrue(ScriptDetector.hasScripts(parent));
  }

  @Test
  void returnsFalseForNestedPlanWithoutScripts() {
    AbstractCalciteIndexScan scan = createMockScan(false);
    RelNode parent = createMockNode(scan);
    assertFalse(ScriptDetector.hasScripts(parent));
  }

  private static RelNode createMockNode(RelNode... children) {
    RelNode node = mock(RelNode.class);
    List<RelNode> childList = List.of(children);
    when(node.getInputs()).thenReturn(childList);
    doAnswer(
            invocation -> {
              RelVisitor visitor = invocation.getArgument(0);
              for (int i = 0; i < childList.size(); i++) {
                visitor.visit(childList.get(i), i, node);
              }
              return null;
            })
        .when(node)
        .childrenAccept(any(RelVisitor.class));
    return node;
  }

  private static AbstractCalciteIndexScan createMockScan(boolean scriptPushed) {
    AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class);
    when(scan.isScriptPushed()).thenReturn(scriptPushed);
    when(scan.getInputs()).thenReturn(List.of());
    doAnswer(
            invocation -> {
              return null;
            })
        .when(scan)
        .childrenAccept(any(RelVisitor.class));
    return scan;
  }
}

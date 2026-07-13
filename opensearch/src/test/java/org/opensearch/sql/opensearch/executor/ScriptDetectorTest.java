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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.AggSpec;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ScriptDetectorTest {

  @Test
  void returnsFalseForNonScanNode() {
    RelNode mockNode = createMockNode();
    assertFalse(ScriptDetector.hasScripts(mockNode));
  }

  @Test
  void returnsTrueWhenFilterScriptPushed() {
    AbstractCalciteIndexScan scan = createMockScan(true, false, 0);
    assertTrue(ScriptDetector.hasScripts(scan));
  }

  @Test
  void returnsTrueWhenAggScriptPresent() {
    AbstractCalciteIndexScan scan = createMockScan(false, false, 1);
    assertTrue(ScriptDetector.hasScripts(scan));
  }

  @Test
  void returnsTrueWhenSortExprPushed() {
    AbstractCalciteIndexScan scan = createMockScan(false, true, 0);
    assertTrue(ScriptDetector.hasScripts(scan));
  }

  @Test
  void returnsFalseWhenNoScripts() {
    AbstractCalciteIndexScan scan = createMockScan(false, false, 0);
    assertFalse(ScriptDetector.hasScripts(scan));
  }

  @Test
  void detectsScriptsInNestedPlan() {
    AbstractCalciteIndexScan scan = createMockScan(false, false, 3);
    RelNode parent = createMockNode(scan);
    assertTrue(ScriptDetector.hasScripts(parent));
  }

  @Test
  void detectsJoinNode() {
    LogicalJoin join = mock(LogicalJoin.class);
    when(join.getJoinType()).thenReturn(JoinRelType.LEFT);
    when(join.getInputs()).thenReturn(List.of());
    doAnswer(invocation -> null).when(join).childrenAccept(any(RelVisitor.class));
    assertTrue(ScriptDetector.hasScripts(join));
  }

  @Test
  void detectsWindowNode() {
    LogicalWindow window = mock(LogicalWindow.class);
    when(window.getInputs()).thenReturn(List.of());
    doAnswer(invocation -> null).when(window).childrenAccept(any(RelVisitor.class));
    assertTrue(ScriptDetector.hasScripts(window));
  }

  @Test
  void detectsUdfInProject() {
    SqlUserDefinedFunction udf = mock(SqlUserDefinedFunction.class);
    RexCall udfCall = mock(RexCall.class);
    when(udfCall.getOperator()).thenReturn(udf);
    when(udfCall.getOperands()).thenReturn(List.of());
    RelDataType type = mock(RelDataType.class);
    when(udfCall.getType()).thenReturn(type);
    doAnswer(inv -> inv.<org.apache.calcite.rex.RexVisitor<?>>getArgument(0).visitCall(udfCall))
        .when(udfCall)
        .accept(any());

    LogicalProject project = mock(LogicalProject.class);
    when(project.getProjects()).thenReturn(List.of(udfCall));
    when(project.getInputs()).thenReturn(List.of());
    doAnswer(invocation -> null).when(project).childrenAccept(any(RelVisitor.class));

    assertTrue(ScriptDetector.hasScripts(project));
  }

  @Test
  void detectsRexOverInProject() {
    RexOver rexOver = mock(RexOver.class);
    SqlOperator op = mock(SqlOperator.class);
    when(rexOver.getOperator()).thenReturn(op);
    when(rexOver.getOperands()).thenReturn(List.of());
    RelDataType type = mock(RelDataType.class);
    when(rexOver.getType()).thenReturn(type);
    doAnswer(inv -> inv.<org.apache.calcite.rex.RexVisitor<?>>getArgument(0).visitCall(rexOver))
        .when(rexOver)
        .accept(any());

    LogicalProject project = mock(LogicalProject.class);
    when(project.getProjects()).thenReturn(List.of(rexOver));
    when(project.getInputs()).thenReturn(List.of());
    doAnswer(invocation -> null).when(project).childrenAccept(any(RelVisitor.class));

    assertTrue(ScriptDetector.hasScripts(project));
  }

  @Test
  void returnsFalseForSimpleFieldProject() {
    RexInputRef fieldRef = mock(RexInputRef.class);
    RelDataType type = mock(RelDataType.class);
    when(fieldRef.getType()).thenReturn(type);

    LogicalProject project = mock(LogicalProject.class);
    when(project.getProjects()).thenReturn(List.of((RexNode) fieldRef));
    when(project.getInputs()).thenReturn(List.of());
    doAnswer(invocation -> null).when(project).childrenAccept(any(RelVisitor.class));

    assertFalse(ScriptDetector.hasScripts(project));
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

  private static AbstractCalciteIndexScan createMockScan(
      boolean scriptPushed, boolean sortExprPushed, long aggScriptCount) {
    AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class);

    PushDownContext ctx = mock(PushDownContext.class);
    when(ctx.isScriptPushed()).thenReturn(scriptPushed);
    when(ctx.isSortExprPushed()).thenReturn(sortExprPushed);

    if (aggScriptCount > 0) {
      AggSpec aggSpec = mock(AggSpec.class);
      when(aggSpec.getScriptCount()).thenReturn(aggScriptCount);
      when(ctx.getAggSpec()).thenReturn(aggSpec);
    } else {
      when(ctx.getAggSpec()).thenReturn(null);
    }

    when(scan.getPushDownContext()).thenReturn(ctx);
    when(scan.getInputs()).thenReturn(List.of());
    doAnswer(invocation -> null).when(scan).childrenAccept(any(RelVisitor.class));
    return scan;
  }
}

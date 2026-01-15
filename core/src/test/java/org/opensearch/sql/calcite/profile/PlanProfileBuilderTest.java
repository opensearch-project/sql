/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.profile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.calcite.plan.Scannable;

@ExtendWith(MockitoExtension.class)
class PlanProfileBuilderTest {

  @Mock private RelOptCluster cluster;
  private RelTraitSet traitSet;

  @BeforeEach
  void setUp() {
    traitSet = RelTraitSet.createEmpty();
  }

  @Test
  void wrapsEnumerableRel() {
    EnumerableRel rel = mockEnumerableRel("EnumerableCalc", List.of());

    PlanProfileBuilder.ProfilePlan plan = PlanProfileBuilder.profile((RelNode) rel);

    assertInstanceOf(ProfileEnumerableRel.class, plan.rel());
    assertEquals("EnumerableCalc", plan.planRoot().snapshot().getNode());
  }

  @Test
  void wrapsScannableRel() {
    EnumerableRel rel = mockScannableRel("CalciteEnumerableIndexScan", List.of());

    PlanProfileBuilder.ProfilePlan plan = PlanProfileBuilder.profile((RelNode) rel);

    assertInstanceOf(ProfileScannableRel.class, plan.rel());
    assertEquals("CalciteEnumerableIndexScan", plan.planRoot().snapshot().getNode());
  }

  @Test
  void rebuildsInputsForNonEnumerableRel() {
    EnumerableRel child = mockEnumerableRel("EnumerableCalc", List.of());
    RelNode parent = mock(RelNode.class);
    when(parent.getInputs()).thenReturn(List.of(child));
    when(parent.getRelTypeName()).thenReturn("Parent");
    when(parent.getTraitSet()).thenReturn(traitSet);
    when(parent.copy(any(), anyList())).thenReturn(parent);

    PlanProfileBuilder.ProfilePlan plan = PlanProfileBuilder.profile(parent);

    ArgumentCaptor<List<RelNode>> inputsCaptor = ArgumentCaptor.forClass(List.class);
    verify(parent).copy(any(), inputsCaptor.capture());
    List<RelNode> copiedInputs = inputsCaptor.getValue();
    assertNotNull(copiedInputs);
    assertInstanceOf(ProfileEnumerableRel.class, copiedInputs.get(0));
    assertEquals("Parent", plan.planRoot().snapshot().getNode());
  }

  private EnumerableRel mockEnumerableRel(String name, List<RelNode> inputs) {
    EnumerableRel rel = mock(EnumerableRel.class);
    when(rel.getRelTypeName()).thenReturn(name);
    when(rel.getInputs()).thenReturn(inputs);
    when(rel.getCluster()).thenReturn(cluster);
    when(rel.getTraitSet()).thenReturn(traitSet);
    return rel;
  }

  private EnumerableRel mockScannableRel(String name, List<RelNode> inputs) {
    EnumerableRel rel =
        mock(
            EnumerableRel.class,
            org.mockito.Mockito.withSettings().extraInterfaces(Scannable.class));
    when(rel.getRelTypeName()).thenReturn(name);
    when(rel.getInputs()).thenReturn(inputs);
    when(rel.getCluster()).thenReturn(cluster);
    when(rel.getTraitSet()).thenReturn(traitSet);
    return rel;
  }
}

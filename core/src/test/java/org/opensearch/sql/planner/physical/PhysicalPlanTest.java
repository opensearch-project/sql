/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.mockito.Mockito.verify;

import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.storage.split.Split;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PhysicalPlanTest {
  @Mock Split split;

  @Mock PhysicalPlan child;

  private PhysicalPlan testPlan =
      new PhysicalPlan() {
        @Override
        public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
          throw new UnsupportedOperationException();
        }

        @Override
        public ExprValue next() {
          throw new UnsupportedOperationException();
        }

        @Override
        public List<PhysicalPlan> getChild() {
          return List.of(child);
        }
      };

  @Test
  void add_split_to_child_by_default() {
    testPlan.add(split);
    verify(child).add(split);
  }
}

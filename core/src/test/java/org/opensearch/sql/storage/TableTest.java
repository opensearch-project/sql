package org.opensearch.sql.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

class TableTest {

  @Test
  void create() {
    assertThrows(UnsupportedOperationException.class,
        () -> new Table() {
          @Override
          public Map<String, ExprType> getFieldTypes() {
            return null;
          }

          @Override
          public PhysicalPlan implement(LogicalPlan plan) {
            return null;
          }
        }.create(ImmutableMap.of()));
  }
}
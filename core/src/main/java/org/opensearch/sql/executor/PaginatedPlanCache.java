/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.executor.Cursor;
import org.opensearch.sql.planner.PaginateOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

@RequiredArgsConstructor
public class PaginatedPlanCache {
  private final StorageEngine storageEngine;
  public static final PaginatedPlanCache None = new PaginatedPlanCache(null);

  @RequiredArgsConstructor
  @Data
  static class SeriazationContext {
    private final PaginatedPlanCache cache;
  }

  public static class SerializationVisitor
      extends PhysicalPlanNodeVisitor<byte[], SeriazationContext> {
    private static final byte[] NO_CURSOR = new byte[] {};

    @Override
    public byte[] visitPaginate(PaginateOperator node, SeriazationContext context) {
      // Save cursor to read the next page.
      // Could process node.getChild() here with another visitor -- one that saves the
      // parameters for other physical operators -- ProjectOperator, etc.
      return String.format("You got it!%d", node.getPageIndex() + 1).getBytes();
    }

    // Cursor is returned only if physical plan node is PaginateOerator.
    @Override
    protected byte[] visitNode(PhysicalPlan node, SeriazationContext context) {
      return NO_CURSOR;
    }
  }

  /**
   * Converts a physical plan tree to a cursor. May cache plan related data somewhere.
   */
  public Cursor convertToCursor(PhysicalPlan plan) {
    var serializer = new SerializationVisitor();
    var raw = plan.accept(serializer, new SeriazationContext(this));
    return new Cursor(raw);
  }

  /**
    * Convers a cursor to a physical plann tree.
    */
  public PhysicalPlan convertToPlan(String cursor) {
    // TODO HACKY_HACK -- create a plan
    if (cursor.startsWith("You got it!")) {
      int pageIndex = Integer.parseInt(cursor.substring("You got it!".length()));

      Table table = storageEngine.getTable(null, "phrases");
      TableScanBuilder scanBuilder = table.createScanBuilder();
      scanBuilder.pushDownOffset(5 * pageIndex);
      PhysicalPlan scan = scanBuilder.build();
      var fields = table.getFieldTypes();
      List<NamedExpression> references =
          Stream.of("phrase", "test field", "insert_time2")
              .map(c ->
                  new NamedExpression(c, new ReferenceExpression(c, List.of(c), fields.get(c))))
              .collect(Collectors.toList());

      return new PaginateOperator(new ProjectOperator(scan, references, List.of()), 5, pageIndex);

    } else {
      throw new RuntimeException("Unsupported cursor");
    }
  }
}

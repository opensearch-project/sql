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
      return String.format("You got it!%d", node.getPageIndex() + 1).getBytes();
    }

    @Override
    protected byte[] visitNode(PhysicalPlan node, SeriazationContext context) {
      return NO_CURSOR;
    }
  }

  public Cursor convertToCursor(PhysicalPlan plan) {
    var serializer = new SerializationVisitor();
    var raw = plan.accept(serializer, new SeriazationContext(this));
    return new Cursor(raw);
  }

  public PhysicalPlan convertToPlan(String cursor) {
    // TODO HACKY_HACK -- create a plan
    if (cursor.startsWith("You got it!")) {
      int pageIndex = Integer.parseInt(cursor.substring("You got it!".length()));

      Table table = storageEngine.getTable(null, "phrases");
      TableScanBuilder scanBuilder = table.createScanBuilder();
      scanBuilder.pushDownOffset(5*pageIndex);
      PhysicalPlan scan = scanBuilder.build();
      var fields = table.getFieldTypes();
      List<NamedExpression> references =
          Stream.of("phrase", "test field", "insert_time2")
              .map(c ->
                  new NamedExpression(c, new ReferenceExpression(c, List.of(c), fields.get(c))))
              .collect(Collectors.toList());
//      List<NamedExpression> references = List.of(
//          new NamedExpression("phrase",
//              new ReferenceExpression("phrase",List.of("phrase"), opensearchTextType))
//          , new NamedExpression("test field",
//              new ReferenceExpression("test field", List.of("test field"), LONG))
//          , new NamedExpression("insert_time2",
//              new ReferenceExpression("insert_time2", List.of("insert_time2"), TIMESTAMP))
//      );

      return new PaginateOperator(new ProjectOperator(scan, references, List.of()), 5, pageIndex);

    } else {
      throw new RuntimeException("Unsupported cursor");
    }
  }

  private PhysicalPlan deserializePlan(byte[] ba) {
    try (ByteArrayInputStream bi = new ByteArrayInputStream(ba);
         ObjectInputStream oi = new ObjectInputStream(bi)) {
      return (PhysicalPlan) oi.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}

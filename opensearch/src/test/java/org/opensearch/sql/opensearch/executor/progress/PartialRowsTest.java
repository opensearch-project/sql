/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.job.QueryResult;

class PartialRowsTest {

  private static final Schema SCHEMA = new Schema(List.of());

  @Test
  void emptyBeforeOpenAndBeforeFirstRow() {
    PartialRows rows = new PartialRows();
    rows.add(integerValue(1));
    assertTrue(rows.snapshot().isEmpty());

    rows.open(values -> SCHEMA);
    assertTrue(rows.snapshot().isEmpty());
  }

  @Test
  void snapshotHoldsRowsSoFarWithSchemaOfThoseRows() {
    PartialRows rows = new PartialRows();
    List<Integer> schemaInputSizes = new ArrayList<>();
    rows.open(
        values -> {
          schemaInputSizes.add(values.size());
          return SCHEMA;
        });
    rows.add(integerValue(1));
    rows.add(integerValue(2));

    QueryResult.Rows snapshot = rows.snapshot().orElseThrow();
    assertEquals(List.of(integerValue(1), integerValue(2)), snapshot.rows());
    assertEquals(SCHEMA, snapshot.schema());
    assertEquals(List.of(2), schemaInputSizes);
  }

  @Test
  void schemaIsBuiltOnceAndSnapshotReusedWhileNoRowIsAdded() {
    PartialRows rows = new PartialRows();
    List<Integer> schemaCalls = new ArrayList<>();
    rows.open(
        values -> {
          schemaCalls.add(values.size());
          return SCHEMA;
        });
    rows.add(integerValue(1));
    QueryResult.Rows first = rows.snapshot().orElseThrow();
    assertSame(first, rows.snapshot().orElseThrow());
    rows.add(integerValue(2));
    assertEquals(2, rows.snapshot().orElseThrow().rows().size());
    assertEquals(List.of(1), schemaCalls);
  }

  @Test
  void snapshotIsNotChangedByLaterRows() {
    PartialRows rows = new PartialRows();
    rows.open(values -> SCHEMA);
    rows.add(integerValue(1));
    QueryResult.Rows snapshot = rows.snapshot().orElseThrow();
    rows.add(integerValue(2));
    assertEquals(List.of(integerValue(1)), snapshot.rows());
  }

  @Test
  void discardDropsRowsAndIgnoresLaterOnes() {
    PartialRows rows = new PartialRows();
    rows.open(values -> SCHEMA);
    rows.add(integerValue(1));
    rows.discard();
    rows.add(integerValue(2));
    assertTrue(rows.snapshot().isEmpty());
  }

  @Test
  void openStartsOver() {
    PartialRows rows = new PartialRows();
    rows.open(values -> SCHEMA);
    rows.add(integerValue(1));
    rows.open(values -> SCHEMA);
    assertTrue(rows.snapshot().isEmpty());
  }

  @Test
  void concurrentSnapshotsArePrefixesOfTheRows() throws Exception {
    int total = 20_000;
    PartialRows rows = new PartialRows();
    rows.open(values -> SCHEMA);
    CountDownLatch done = new CountDownLatch(1);
    AtomicReference<String> violation = new AtomicReference<>();
    Thread reader =
        new Thread(
            () -> {
              int last = 0;
              while (done.getCount() > 0) {
                List<ExprValue> seen =
                    rows.snapshot().map(QueryResult.Rows::rows).orElse(List.of());
                for (int i = 0; i < seen.size(); i++) {
                  if (seen.get(i).integerValue() != i) {
                    violation.set("row " + i + " is " + seen.get(i));
                  }
                }
                if (seen.size() < last) {
                  violation.set("snapshot shrank from " + last + " to " + seen.size());
                }
                last = seen.size();
              }
            });
    reader.start();
    for (int i = 0; i < total; i++) {
      rows.add(integerValue(i));
    }
    done.countDown();
    reader.join();

    assertEquals(null, violation.get());
    assertEquals(total, rows.snapshot().orElseThrow().rows().size());
  }
}

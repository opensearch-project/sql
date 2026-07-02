/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;

/**
 * Physical pass-through write spool for {@code collect}: forwards each input row downstream and
 * appends it to the destination index via the native bulk path. See the collect RFC for design.
 */
@Getter
public class EnumerableOpenSearchTableSpool extends TableSpool implements EnumerableRel {

  private static final int BATCH_SIZE = 1000;

  /**
   * Resolved once so codegen binds to the declared Enumerable parameter, not AbstractEnumerable.
   */
  private static final Method WRITE_THROUGH =
      Types.lookupMethod(EnumerableOpenSearchTableSpool.class, "writeThrough", Enumerable.class);

  private final OpenSearchIndex osIndex;
  private final String indexName;

  public EnumerableOpenSearchTableSpool(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelOptTable table) {
    super(cluster, traitSet, input, Spool.Type.LAZY, Spool.Type.LAZY, table);
    this.osIndex = table.unwrap(OpenSearchIndex.class);
    this.indexName = table.getQualifiedName().get(table.getQualifiedName().size() - 1);
  }

  @Override
  protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
    return new EnumerableOpenSearchTableSpool(input.getCluster(), traitSet, input, getTable());
  }

  /**
   * Cheaper than Calcite's built-in EnumerableTableSpool so Volcano never picks it (it needs an
   * in-memory ModifiableTable OpenSearch lacks, which would fail at runtime).
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost base = super.computeSelfCost(planner, mq);
    return base == null ? null : base.multiplyBy(0.1);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Consume the child via visitChild (not a Scannable short-circuit) so collect accepts any
    // input shape; a bare scan's own scan() still drives unbounded PIT paging.
    BlockBuilder builder = new BlockBuilder();
    Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            OpenSearchRelOptUtil.replaceDot(getCluster().getTypeFactory(), getRowType()),
            pref.preferArray());
    Expression inputEnumerable = builder.append("inputEnumerable", inputResult.block);
    Expression op = implementor.stash(this, EnumerableOpenSearchTableSpool.class);
    builder.add(Expressions.return_(null, Expressions.call(op, WRITE_THROUGH, inputEnumerable)));
    return implementor.result(physType, builder.toBlock());
  }

  public Enumerable<@Nullable Object> writeThrough(Enumerable<@Nullable Object> input) {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<@Nullable Object> enumerator() {
        return new PassThroughWriter(EnumerableOpenSearchTableSpool.this, input.enumerator());
      }
    };
  }

  /**
   * Drains the entire input on first access, bulk-writing every row, then emits buffered rows for
   * pass-through. The write is eager, not lazy, because the plan's top LogicalSystemLimit
   * (QUERY_SIZE_LIMIT) caps the client result set; a lazy write would stop at that cap and never
   * write the rest. Pass-through is buffered up to QUERY_SIZE_LIMIT to bound memory.
   */
  private static class PassThroughWriter implements Enumerator<@Nullable Object> {

    private final OpenSearchClient client;
    private final String indexName;
    private final List<String> fields;
    private final Enumerator<@Nullable Object> source;
    private final int passThroughCap;
    private BulkRequest bulk = new BulkRequest();
    private int buffered = 0;
    private @Nullable Object current = null;
    private boolean drained = false;
    private Iterator<@Nullable Object> passThrough = null;

    PassThroughWriter(EnumerableOpenSearchTableSpool spool, Enumerator<@Nullable Object> source) {
      this.client = spool.getOsIndex().getClient();
      this.indexName = spool.getIndexName();
      this.fields = spool.getInput().getRowType().getFieldNames();
      this.passThroughCap = spool.getOsIndex().getSettings().getSettingValue(Key.QUERY_SIZE_LIMIT);
      this.source = source;
    }

    @Override
    public Object current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      if (!drained) {
        drainAndWrite();
        drained = true;
      }
      if (passThrough.hasNext()) {
        current = passThrough.next();
        return true;
      }
      return false;
    }

    private void drainAndWrite() {
      List<@Nullable Object> passThroughBuffer = new ArrayList<>();
      while (source.moveNext()) {
        Object row = source.current();
        bulk.add(toIndexRequest(row));
        if (++buffered >= BATCH_SIZE) {
          flush();
        }
        if (passThroughBuffer.size() < passThroughCap) {
          passThroughBuffer.add(row);
        }
      }
      if (buffered > 0) {
        flush();
      }
      passThrough = passThroughBuffer.iterator();
    }

    private void flush() {
      bulkWithRetry(client, bulk);
      bulk = new BulkRequest();
      buffered = 0;
    }

    /**
     * Retries only 429-rejected items (indexing backpressure) under exponential backoff; other
     * per-item failures stay surfaced in the response, and the rows have already passed through.
     */
    private static void bulkWithRetry(OpenSearchClient client, BulkRequest request) {
      java.util.Iterator<TimeValue> backoff = BackoffPolicy.exponentialBackoff().iterator();
      BulkRequest pending = request;
      while (true) {
        BulkResponse response = client.bulk(pending);
        if (!response.hasFailures()) {
          return;
        }
        BulkRequest retry = new BulkRequest();
        for (BulkItemResponse item : response.getItems()) {
          if (item.isFailed() && item.getFailure().getStatus() == RestStatus.TOO_MANY_REQUESTS) {
            retry.add(pending.requests().get(item.getItemId()));
          }
        }
        if (retry.numberOfActions() == 0 || !backoff.hasNext()) {
          return;
        }
        try {
          Thread.sleep(backoff.next().millis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        pending = retry;
      }
    }

    private IndexRequest toIndexRequest(@Nullable Object row) {
      Object[] values = (row instanceof Object[] arr) ? arr : new Object[] {row};
      Map<String, Object> doc = new LinkedHashMap<>();
      for (int i = 0; i < fields.size() && i < values.length; i++) {
        String name = fields.get(i);
        if (values[i] != null && !OpenSearchIndex.METADATAFIELD_TYPE_MAP.containsKey(name)) {
          doc.put(name, values[i]);
        }
      }
      return new IndexRequest(indexName).source(doc);
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      source.close();
    }
  }
}

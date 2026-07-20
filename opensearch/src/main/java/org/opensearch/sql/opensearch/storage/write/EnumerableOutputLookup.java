/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import java.lang.reflect.Method;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.opensearch.storage.write.WriteConfig.WriteMode;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Physical terminal write operator for {@code outputlookup}. Drains the child rows, materializes
 * them into the lookup via {@link OutputLookupWriteExec}, and emits a single {@code BIGINT} row
 * carrying the written count.
 */
@Getter
public class EnumerableOutputLookup extends SingleRel implements EnumerableRel {

  private static final Method WRITE_AND_COUNT =
      Types.lookupMethod(EnumerableOutputLookup.class, "writeAndCount", Enumerable.class);

  private final transient NodeClient client;
  private final RelDataType modifyRowType;
  private final String indexName;
  private final boolean append;
  private final boolean overrideIfEmpty;
  private final List<String> keyFields;
  private final @Nullable Integer max;
  private final int maxRows;
  private final List<String> fields;

  public EnumerableOutputLookup(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType modifyRowType,
      NodeClient client,
      String indexName,
      boolean append,
      boolean overrideIfEmpty,
      List<String> keyFields,
      @Nullable Integer max,
      int maxRows) {
    super(cluster, traits, input);
    this.modifyRowType = modifyRowType;
    this.client = client;
    this.indexName = indexName;
    this.append = append;
    this.overrideIfEmpty = overrideIfEmpty;
    this.keyFields = keyFields;
    this.max = max;
    this.maxRows = maxRows;
    this.fields = input.getRowType().getFieldNames();
  }

  @Override
  protected RelDataType deriveRowType() {
    return modifyRowType;
  }

  /**
   * Surface the write parameters in explain output so the physical plan shows the write target and
   * mode (the write itself is performed at runtime by {@link OutputLookupWriteExec}, which is not a
   * relational operator and therefore never appears as a plan node). Including these in the digest
   * also keeps two outputlookups with different targets or modes from being deduplicated by the
   * planner.
   */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("dest", indexName)
        .itemIf("key_field", keyFields, !keyFields.isEmpty())
        .item("append", append)
        .item("override_if_empty", overrideIfEmpty)
        .itemIf("max", max, max != null);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableOutputLookup(
        getCluster(),
        traitSet,
        inputs.get(0),
        modifyRowType,
        client,
        indexName,
        append,
        overrideIfEmpty,
        keyFields,
        max,
        maxRows);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    BlockBuilder builder = new BlockBuilder();
    Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
    PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
    Expression inputEnumerable = builder.append("inputEnumerable", inputResult.block);
    Expression op = implementor.stash(this, EnumerableOutputLookup.class);
    builder.add(Expressions.return_(null, Expressions.call(op, WRITE_AND_COUNT, inputEnumerable)));
    return implementor.result(physType, builder.toBlock());
  }

  /** Runtime entry: drain input, materialize into the lookup, emit one row with the count. */
  public Enumerable<@Nullable Object> writeAndCount(Enumerable<@Nullable Object> input) {
    WriteMode mode = keyFields.isEmpty() ? WriteMode.APPEND : WriteMode.UPSERT;
    long count =
        OutputLookupWriteExec.execute(
            client,
            indexName,
            fields,
            mode,
            keyFields,
            max,
            maxRows,
            overrideIfEmpty,
            append,
            input.enumerator());
    return Linq4j.asEnumerable(List.of((Object) count));
  }
}

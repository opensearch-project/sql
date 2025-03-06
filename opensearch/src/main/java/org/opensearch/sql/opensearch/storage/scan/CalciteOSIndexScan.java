/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static java.util.Objects.requireNonNull;

import java.util.ArrayDeque;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Relational expression representing a scan of an OpenSearchIndex type. */
public abstract class CalciteOSIndexScan extends TableScan {
  @Getter protected final OpenSearchIndex osIndex;
  // The schema of this scan operator, it's initialized with the row type of the table, but may be
  // changed by push down operations.
  @Getter protected final RelDataType schema;
  // This context maintains all the push down actions, which will be applied to the requestBuilder
  // when it begins to scan data from OpenSearch.
  // Because OpenSearchRequestBuilder doesn't support deep copy while we want to keep the
  // requestBuilder independent among different plans produced in the optimization process,
  // so we cannot apply these actions right away.
  @Getter protected final PushDownContext pushDownContext;

  protected CalciteOSIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, traitSet, hints, table);
    this.osIndex = requireNonNull(osIndex, "OpenSearch index");
    ;
    this.schema = schema;
    this.pushDownContext = pushDownContext;
  }

  @Override
  public RelDataType deriveRowType() {
    return this.schema;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("PushDownContext", pushDownContext, !pushDownContext.isEmpty());
  }

  // TODO: should we consider equivalent among PushDownContexts with different push down sequence?
  public static class PushDownContext extends ArrayDeque<PushDownAction> {
    private boolean isAggregatePushed = false;

    @Override
    public PushDownContext clone() {
      return (PushDownContext) super.clone();
    }

    @Override
    public boolean add(PushDownAction pushDownAction) {
      // Defense check. I never do push down to this context after aggregate push-down.
      assert !isAggregatePushed : "Aggregate has already been pushed!";
      if (pushDownAction.type == PushDownType.AGGREGATION) {
        isAggregatePushed = true;
      }
      return super.add(pushDownAction);
    }

    public boolean isAggregatePushed() {
      if (isAggregatePushed) return true;
      isAggregatePushed = !isEmpty() && super.peekLast().type == PushDownType.AGGREGATION;
      return isAggregatePushed;
    }
  }

  protected enum PushDownType {
    FILTER,
    PROJECT,
    AGGREGATION,
    // SORT,
    // LIMIT,
    // HIGHLIGHT,
    // NESTED
  }

  public record PushDownAction(PushDownType type, Object digest, AbstractAction action) {
    static PushDownAction of(PushDownType type, Object digest, AbstractAction action) {
      return new PushDownAction(type, digest, action);
    }

    public String toString() {
      return type + ":" + digest;
    }

    public void apply(OpenSearchRequestBuilder requestBuilder) {
      action.apply(requestBuilder);
    }
  }

  public interface AbstractAction {
    void apply(OpenSearchRequestBuilder requestBuilder);
  }
}

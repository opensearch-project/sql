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
@Getter
public abstract class CalciteIndexScan extends TableScan {
  protected final OpenSearchIndex osIndex;
  // The schema of this scan operator, it's initialized with the row type of the table, but may be
  // changed by push down operations.
  protected final RelDataType schema;
  // This context maintains all the push down actions, which will be applied to the requestBuilder
  // when it begins to scan data from OpenSearch.
  // Because OpenSearchRequestBuilder doesn't support deep copy while we want to keep the
  // requestBuilder independent among different plans produced in the optimization process,
  // so we cannot apply these actions right away.
  protected final PushDownContext pushDownContext;

  protected CalciteIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, traitSet, hints, table);
    this.osIndex = requireNonNull(osIndex, "OpenSearch index");
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
      // Defense check. It should never do push down to this context after aggregate push-down.
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

  public class PushDownAction {

    private final PushDownType type;
    private final Object digest;
    private final AbstractAction action;

    PushDownAction(PushDownType type, Object digest, AbstractAction action) {
      this.type = type;
      this.digest = digest;
      this.action = action;
    }

    @Override
    public String toString() {
      return type + ":" + digest;
    }

    public void apply(OpenSearchRequestBuilder requestBuilder) {
      action.apply(requestBuilder);
    }

    public PushDownType getType() {
      return type;
    }

    public Object getDigest() {
      return digest;
    }

    public AbstractAction getAction() {
      return action;
    }
  }

  public interface AbstractAction {
    void apply(OpenSearchRequestBuilder requestBuilder);
  }
}

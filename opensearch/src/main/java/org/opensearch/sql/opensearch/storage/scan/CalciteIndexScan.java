/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.common.setting.Settings.Key.QUERY_SIZE_LIMIT;

import java.util.ArrayDeque;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NumberUtil;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Relational expression representing a scan of an OpenSearchIndex type. */
@Getter
public abstract class CalciteIndexScan extends TableScan {
  public final OpenSearchIndex osIndex;
  // The schema of this scan operator, it's initialized with the row type of the table, but may be
  // changed by push down operations.
  protected final RelDataType schema;
  // This context maintains all the push down actions, which will be applied to the requestBuilder
  // when it begins to scan data from OpenSearch.
  // Because OpenSearchRequestBuilder doesn't support deep copy while we want to keep the
  // requestBuilder independent among different plans produced in the optimization process,
  // so we cannot apply these actions right away.
  protected final PushDownContext pushDownContext;

  /**
   * The estimated row count of this index scan operator. Initialed with the value of setting {@link
   * QUERY_SIZE_LIMIT}, and will be updated by the push down actions when estimating.
   */
  private Double rowCount;

  /** The status to indicate whether the row count has been estimated. */
  private boolean rowCountEstimated = false;

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
    this.rowCount =
        ((Integer) (osIndex.getSettings().getSettingValue(QUERY_SIZE_LIMIT))).doubleValue();
  }

  @Override
  public RelDataType deriveRowType() {
    return this.schema;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    OpenSearchRequestBuilder requestBuilder = osIndex.createRequestBuilder();
    pushDownContext.forEach(action -> action.apply(requestBuilder));
    String explainString = pushDownContext + ", " + requestBuilder;
    return super.explainTerms(pw)
        .itemIf("PushDownContext", explainString, !pushDownContext.isEmpty());
  }

  /**
   * The impact factor to estimate the row count after push down an operator.
   *
   * <p>It will be multiplied to the original estimated row count of the operator, and it's set to
   * **0.9**, so it will always be less than the row count of operator without push down.
   *
   * <p>As a result, the optimizer will prefer the plan with push down.
   */
  public static final double estimateRowCountFactor = 0.9;

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    if (!rowCountEstimated) {
      pushDownContext.forEach(
          action ->
              rowCount =
                  switch (action.type) {
                        case AGGREGATION -> mq.getRowCount((RelNode) action.digest);
                        case PROJECT -> rowCount;
                        case FILTER -> NumberUtil.multiply(
                            rowCount, mq.getSelectivity(this, (RexNode) action.digest));
                      }
                      * estimateRowCountFactor);
      rowCountEstimated = true;
    }
    return rowCount;
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

  public record PushDownAction(PushDownType type, Object digest, AbstractAction action) {
    static PushDownAction of(PushDownType type, Object digest, AbstractAction action) {
      return new PushDownAction(type, digest, action);
    }

    public String toString() {
      return type + "->" + digest;
    }

    public void apply(OpenSearchRequestBuilder requestBuilder) {
      action.apply(requestBuilder);
    }
  }

  public interface AbstractAction {
    void apply(OpenSearchRequestBuilder requestBuilder);
  }
}

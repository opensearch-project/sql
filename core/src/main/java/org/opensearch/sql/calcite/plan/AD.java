/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import static org.opensearch.sql.utils.MLCommonsConstants.RCF_ANOMALOUS;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_ANOMALY_GRADE;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_SCORE;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_FIELD;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public abstract class AD extends SingleRel {

  @Getter private final ImmutableMap<String, Object> arguments;
  private final boolean isTimeSeries;

  private static final String DUPLICATE_RCF_SCORE = RCF_SCORE + "1";
  private static final String DUPLICATE_RCF_ANOMALY_GRADE = RCF_ANOMALY_GRADE + "1";
  private static final String DUPLICATE_RCF_ANOMALOUS = RCF_ANOMALOUS + "1";

  /**
   * Creates an AD operator
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits collation traits of the operator, usually NONE for ad
   * @param input Input relational expression
   * @param arguments an argument mapping of parameter keys and values
   */
  protected AD(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, Map<String, Object> arguments) {
    super(cluster, traits, input);
    this.arguments = ImmutableMap.copyOf(arguments);
    this.isTimeSeries = arguments.containsKey(TIME_FIELD);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return copy(traitSet, getInputs());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("arguments", arguments);
  }

  @Override
  public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public abstract AD copy(RelTraitSet traitSet, RelNode input);

  @Override
  protected RelDataType deriveRowType() {
    RelDataType inputRowType = getInput().getRowType();
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    String scoreName =
        inputRowType.getFieldNames().contains(RCF_SCORE) ? DUPLICATE_RCF_SCORE : RCF_SCORE;
    if (isTimeSeries) {
      String anomalyGradeName =
          inputRowType.getFieldNames().contains(RCF_ANOMALY_GRADE)
              ? DUPLICATE_RCF_ANOMALY_GRADE
              : RCF_ANOMALY_GRADE;
      return typeFactory
          .builder()
          .kind(inputRowType.getStructKind())
          .addAll(inputRowType.getFieldList())
          .add(scoreName, SqlTypeName.DOUBLE)
          .add(anomalyGradeName, SqlTypeName.DOUBLE)
          .build();
    } else {
      String anomalousName =
          inputRowType.getFieldNames().contains(RCF_ANOMALOUS)
              ? DUPLICATE_RCF_ANOMALOUS
              : RCF_ANOMALOUS;
      return typeFactory
          .builder()
          .kind(inputRowType.getStructKind())
          .addAll(inputRowType.getFieldList())
          .add(scoreName, SqlTypeName.DOUBLE)
          .add(anomalousName, SqlTypeName.BOOLEAN)
          .build();
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double dRows = mq.getRowCount(getInput());
    double dCpu = 0; // Assume it's remote cluster AD algorithm cost
    double dIo = dRows * 2; // nodeClient request round trip network IO cost
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }
}

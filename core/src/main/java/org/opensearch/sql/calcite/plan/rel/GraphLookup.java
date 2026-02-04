/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Abstract RelNode for graphLookup command.
 *
 * <p>Has two inputs:
 *
 * <ul>
 *   <li>source: source table (rows to start BFS from)
 *   <li>lookup: lookup table (graph edges to traverse)
 * </ul>
 *
 * <p>At execution time, performs BFS by dynamically querying OpenSearch with filter pushdown
 * instead of loading all lookup data into memory.
 *
 * <p>This is a storage-agnostic logical node. Storage-specific implementations (e.g., for
 * OpenSearch) should extract the necessary index information from the lookup RelNode during
 * conversion to the physical plan.
 */
@Getter
public abstract class GraphLookup extends BiRel {

  // TODO: use RexInputRef instead of String for there fields
  protected final String startField; // Field in source table (start entities)
  protected final String fromField; // Field in lookup table (edge source)
  protected final String toField; // Field in lookup table (edge target)
  protected final String outputField; // Name of output array field
  @Nullable protected final String depthField; // Name of output array field

  // TODO: add limitation on the maxDepth and input rows count
  protected final int maxDepth; // -1 = unlimited
  protected final boolean bidirectional;

  private RelDataType outputRowType;

  /**
   * Creates a LogicalGraphLookup.
   *
   * @param cluster Cluster
   * @param traitSet Trait set
   * @param source Source table RelNode
   * @param lookup Lookup table RelNode
   * @param startField Field name for start entities
   * @param fromField Field name for outgoing edges
   * @param toField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param depthField Name of the depth field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   */
  protected GraphLookup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      RelNode lookup,
      String startField,
      String fromField,
      String toField,
      String outputField,
      @Nullable String depthField,
      int maxDepth,
      boolean bidirectional) {
    super(cluster, traitSet, source, lookup);
    this.startField = startField;
    this.fromField = fromField;
    this.toField = toField;
    this.outputField = outputField;
    this.depthField = depthField;
    this.maxDepth = maxDepth;
    this.bidirectional = bidirectional;
  }

  /** Returns the source table RelNode. */
  public RelNode getSource() {
    return left;
  }

  /** Returns the lookup table RelNode. */
  public RelNode getLookup() {
    return right;
  }

  @Override
  public abstract RelNode copy(RelTraitSet traitSet, List<RelNode> inputs);

  @Override
  protected RelDataType deriveRowType() {
    if (outputRowType == null) {
      // Output = source fields + output array field
      RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

      // Add all source fields
      for (var field : getSource().getRowType().getFieldList()) {
        builder.add(field);
      }

      // Add output field (ARRAY type containing lookup row struct)
      RelDataType lookupRowType = getLookup().getRowType();
      if (this.depthField != null) {
        final RelDataTypeFactory.Builder lookupBuilder = getCluster().getTypeFactory().builder();
        lookupBuilder.addAll(lookupRowType.getFieldList());
        RelDataType depthType = getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        lookupBuilder.add(this.depthField, depthType);
        lookupRowType = lookupBuilder.build();
      }
      RelDataType arrayType = getCluster().getTypeFactory().createArrayType(lookupRowType, -1);
      builder.add(outputField, arrayType);

      outputRowType = builder.build();
    }
    return outputRowType;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return getSource().estimateRowCount(mq);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("fromField", fromField)
        .item("toField", toField)
        .item("outputField", outputField)
        .item("depthField", depthField)
        .item("maxDepth", maxDepth)
        .item("bidirectional", bidirectional);
  }
}

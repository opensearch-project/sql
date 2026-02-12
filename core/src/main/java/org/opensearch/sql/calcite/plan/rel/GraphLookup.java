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
import org.apache.calcite.rex.RexNode;
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
  protected final String startWith; // Field in source table (start entities)
  protected final String connectFromField; // Field in lookup table (edge source)
  protected final String connectToField; // Field in lookup table (edge target)
  protected final String outputField; // Name of output array field
  @Nullable protected final String depthField; // Name of output array field

  // TODO: add limitation on the maxDepth and input rows count
  protected final int maxDepth; // -1 = unlimited
  protected final boolean bidirectional;
  protected final boolean supportArray;
  protected final boolean batchMode;
  protected final boolean usePIT;
  @Nullable protected final RexNode filter;

  private RelDataType outputRowType;

  /**
   * Creates a LogicalGraphLookup.
   *
   * @param cluster Cluster
   * @param traitSet Trait set
   * @param source Source table RelNode
   * @param lookup Lookup table RelNode
   * @param startWith Field name for start entities
   * @param connectFromField Field name for outgoing edges
   * @param connectToField Field name for incoming edges
   * @param outputField Name of the output array field
   * @param depthField Name of the depth field
   * @param maxDepth Maximum traversal depth (-1 for unlimited)
   * @param bidirectional Whether to traverse edges in both directions
   * @param supportArray Whether to support array-typed fields (disables early visited filter
   *     pushdown)
   * @param batchMode Whether to batch all source start values into a single unified BFS
   * @param usePIT Whether to use PIT (Point In Time) search for complete results
   * @param filter Optional filter condition for lookup table documents
   */
  protected GraphLookup(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      RelNode lookup,
      String startWith,
      String connectFromField,
      String connectToField,
      String outputField,
      @Nullable String depthField,
      int maxDepth,
      boolean bidirectional,
      boolean supportArray,
      boolean batchMode,
      boolean usePIT,
      @Nullable RexNode filter) {
    super(cluster, traitSet, source, lookup);
    this.startWith = startWith;
    this.connectFromField = connectFromField;
    this.connectToField = connectToField;
    this.outputField = outputField;
    this.depthField = depthField;
    this.maxDepth = maxDepth;
    this.bidirectional = bidirectional;
    this.supportArray = supportArray;
    this.batchMode = batchMode;
    this.usePIT = usePIT;
    this.filter = filter;
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
      RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

      if (batchMode) {
        // Batch mode: Output = [Array<source>, Array<lookup>]
        // First field: aggregated source rows as array
        RelDataType sourceRowType = getSource().getRowType();
        RelDataType sourceArrayType =
            getCluster().getTypeFactory().createArrayType(sourceRowType, -1);
        builder.add(startWith, sourceArrayType);

        // Second field: aggregated lookup rows as array
        RelDataType lookupRowType = getLookup().getRowType();
        if (this.depthField != null) {
          final RelDataTypeFactory.Builder lookupBuilder = getCluster().getTypeFactory().builder();
          lookupBuilder.addAll(lookupRowType.getFieldList());
          RelDataType depthType = getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER);
          lookupBuilder.add(this.depthField, depthType);
          lookupRowType = lookupBuilder.build();
        }
        RelDataType lookupArrayType =
            getCluster().getTypeFactory().createArrayType(lookupRowType, -1);
        builder.add(outputField, lookupArrayType);
      } else {
        // Normal mode: Output = source fields + output array field
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
      }

      outputRowType = builder.build();
    }
    return outputRowType;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    // Batch mode aggregates all source rows into a single output row
    return batchMode ? 1 : getSource().estimateRowCount(mq);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("connectFromField", connectFromField)
        .item("connectToField", connectToField)
        .item("outputField", outputField)
        .item("depthField", depthField)
        .item("maxDepth", maxDepth)
        .item("bidirectional", bidirectional)
        .itemIf("supportArray", supportArray, supportArray)
        .itemIf("batchMode", batchMode, batchMode)
        .itemIf("usePIT", usePIT, usePIT)
        .itemIf("filter", filter, filter != null);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.converters;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchRelFieldTrimmer;

public class OpenSearchSqlToRelConverter extends SqlToRelConverter {
  protected final RelBuilder relBuilder;

  public OpenSearchSqlToRelConverter(
      RelOptTable.ViewExpander viewExpander,
      @Nullable SqlValidator validator,
      Prepare.CatalogReader catalogReader,
      RelOptCluster cluster,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    this.relBuilder =
        config
            .getRelBuilderFactory()
            .create(
                cluster,
                validator != null ? validator.getCatalogReader().unwrap(RelOptSchema.class) : null)
            .transform(config.getRelBuilderConfigTransform());
  }

  @Override
  protected RelFieldTrimmer newFieldTrimmer() {
    return new OpenSearchRelFieldTrimmer(validator, this.relBuilder);
  }

  /**
   * Override to support Spark SQL's LEFT ANTI JOIN and LEFT SEMI JOIN conversion to RelNode.
   *
   * <p>The default implementation in {@link SqlToRelConverter#convertJoinType} does not expect
   * LEFT_ANTI_JOIN and LEFT_SEMI_JOIN. This override works around the limitation by first
   * temporarily changing LEFT_ANTI_JOIN/LEFT_SEMI_JOIN to LEFT join in the SqlJoin node, then
   * calling {@code super.convertFrom()} to perform normal conversion, finally substituting the join
   * type in the resulting RelNode to ANTI/SEMI.
   *
   * @param bb Scope within which to resolve identifiers
   * @param from FROM clause of a query.
   * @param fieldNames Field aliases, usually come from AS clause, or null
   */
  @Override
  protected void convertFrom(
      Blackboard bb, @Nullable SqlNode from, @Nullable List<String> fieldNames) {
    JoinType originalJoinType = null;
    if (from instanceof SqlJoin join) {
      JoinType joinType = join.getJoinType();
      if (joinType == JoinType.LEFT_SEMI_JOIN || joinType == JoinType.LEFT_ANTI_JOIN) {
        join.setOperand(2, JoinType.LEFT.symbol(from.getParserPosition()));
        originalJoinType = joinType;
      }
    }
    super.convertFrom(bb, from, fieldNames);
    if (originalJoinType != null) {
      RelNode root = bb.root();
      if (root != null) {
        JoinRelType correctJoinType =
            originalJoinType == JoinType.LEFT_SEMI_JOIN ? JoinRelType.SEMI : JoinRelType.ANTI;
        RelNode fixedRoot = modifyJoinType(root, correctJoinType);
        bb.setRoot(fixedRoot, false);
      }
    }
  }

  private RelNode modifyJoinType(RelNode root, JoinRelType correctJoinType) {
    if (root instanceof LogicalProject project) {
      RelNode input = project.getInput();
      RelNode fixedInput = modifyJoinType(input, correctJoinType);
      if (fixedInput != input) {
        return project.copy(
            project.getTraitSet(), fixedInput, project.getProjects(), project.getRowType());
      }
    } else if (root instanceof LogicalJoin join) {
      if (join.getJoinType() == JoinRelType.LEFT) {
        return join.copy(
            join.getTraitSet(),
            join.getCondition(),
            join.getLeft(),
            join.getRight(),
            correctJoinType,
            join.isSemiJoinDone());
      }
    }
    return root;
  }
}

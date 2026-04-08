/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.rel.Dedup;

/**
 * Trims fields from a relational expression.
 *
 * <p>This class extends Calcite's RelFieldTrimmer to support trimming customized operators.
 */
public class OpenSearchRelFieldTrimmer extends RelFieldTrimmer {
  private final RelBuilder openSearchRelBuilder;

  public OpenSearchRelFieldTrimmer(@Nullable SqlValidator validator, RelBuilder relBuilder) {
    super(validator, relBuilder);
    this.openSearchRelBuilder = relBuilder;
  }

  @Override
  public TrimResult trimFields(
      Project project, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = project.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = project.getInput();

    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        ord.e.accept(inputFinder);
      }
    }

    List<RexSubQuery> subQueries = RexUtil.SubQueryCollector.collect(project);
    Set<CorrelationId> correlationIds = RelOptUtil.getVariablesUsed(subQueries);
    ImmutableBitSet requiredColumns = ImmutableBitSet.of();
    if (!correlationIds.isEmpty()) {
      assert correlationIds.size() == 1;
      requiredColumns = RelOptUtil.correlationColumns(correlationIds.iterator().next(), project);
    }

    ImmutableBitSet finderFields = inputFinder.build();
    ImmutableBitSet inputFieldsUsed =
        ImmutableBitSet.builder().addAll(requiredColumns).addAll(finderFields).build();

    TrimResult trimResult = trimChild(project, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    if (newInput == input && fieldsUsed.cardinality() == fieldCount) {
      return result(project, Mappings.createIdentity(fieldCount));
    }

    if (fieldsUsed.cardinality() == 0) {
      return dummyProject(fieldCount, newInput, project);
    }

    final List<RexNode> newProjects = new ArrayList<>();
    final RexVisitor<RexNode> shuttle;
    if (!correlationIds.isEmpty()) {
      assert correlationIds.size() == 1;
      shuttle =
          new RexPermuteInputsShuttle(inputMapping, newInput) {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
              subQuery = (RexSubQuery) super.visitSubQuery(subQuery);
              return RelOptUtil.remapCorrelatesInSuqQuery(
                  openSearchRelBuilder.getRexBuilder(),
                  subQuery,
                  correlationIds.iterator().next(),
                  newInput.getRowType(),
                  inputMapping);
            }
          };
    } else {
      shuttle = new RexPermuteInputsShuttle(inputMapping, newInput);
    }

    final Mapping mapping =
        Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, fieldsUsed.cardinality());
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        mapping.set(ord.i, newProjects.size());
        RexNode newProjectExpr = ord.e.accept(shuttle);
        newProjects.add(newProjectExpr);
      }
    }

    final RelDataType newRowType =
        RelOptUtil.permute(project.getCluster().getTypeFactory(), rowType, mapping);

    if (shouldAvoidSimplifyValues(newProjects, newInput)) {
      return result(
          project.copy(project.getTraitSet(), newInput, newProjects, newRowType), mapping, project);
    }

    openSearchRelBuilder.push(newInput);
    openSearchRelBuilder.project(newProjects, newRowType.getFieldNames(), false, correlationIds);
    return result(openSearchRelBuilder.build(), mapping, project);
  }

  public TrimResult trimFields(
      Dedup dedup, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = dedup.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final List<RexNode> dedupFields = dedup.getDedupeFields();
    RelNode input = dedup.getInput();

    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields, fieldsUsed);
    inputFinder.visitEach(dedup.getDedupeFields());
    final ImmutableBitSet inputFieldsUsed = inputFinder.build();

    // Create input with trimmed columns.
    TrimResult trimResult = trimChild(dedup, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input && fieldsUsed.cardinality() == fieldCount) {
      return result(dedup, Mappings.createIdentity(fieldCount));
    }

    // Build new project expressions, and populate the mapping.
    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(inputMapping, newInput);
    List<RexNode> newDedupFields = shuttle.visitList(dedupFields);

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    return result(dedup.copy(newInput, newDedupFields), inputMapping);
  }

  private boolean shouldAvoidSimplifyValues(List<RexNode> projects, RelNode input) {
    return projects.stream().allMatch(RexLiteral.class::isInstance) && isFixedRowCount(input);
  }

  private boolean isFixedRowCount(RelNode input) {
    if (input instanceof Values) {
      return true;
    }
    if (input instanceof Aggregate aggregate) {
      return aggregate.getGroupSet().isEmpty()
          && aggregate.getGroupType() == Aggregate.Group.SIMPLE;
    }
    return false;
  }
}

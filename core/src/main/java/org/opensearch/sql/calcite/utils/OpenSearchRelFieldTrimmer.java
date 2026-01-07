/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.rel.Dedup;

/**
 * Trims fields from a relational expression.
 *
 * <p>This class extends Calcite's RelFieldTrimmer to support trimming customized operators.
 */
public class OpenSearchRelFieldTrimmer extends RelFieldTrimmer {

  public OpenSearchRelFieldTrimmer(@Nullable SqlValidator validator, RelBuilder relBuilder) {
    super(validator, relBuilder);
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
}

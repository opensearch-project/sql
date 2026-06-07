/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.datetime.DatetimeExtension.UdtMapping;

/**
 * Temporary patch that rewrites datetime UDT return types on RexCall nodes to standard Calcite
 * types. Also re-aligns {@link RexInputRef} declared types against the (already-normalized) child's
 * row type so a parent {@link org.apache.calcite.rel.core.Filter}/{@link
 * org.apache.calcite.rel.core.Project}/{@link org.apache.calcite.rel.core.Aggregate} constructor
 * assertion does not see {@code ref:EXPR_TIMESTAMP VARCHAR input:TIMESTAMP(9)} type mismatch when
 * an upstream node just got its UDT-typed column normalized to a standard type.
 *
 * <p>Not a singleton: {@link RelHomogeneousShuttle} inherits a stateful {@code stack} field from
 * {@link org.apache.calcite.rel.RelShuttleImpl}, so a fresh instance must be used per plan().
 */
class DatetimeUdtNormalizeRule extends RelHomogeneousShuttle {

  @Override
  public RelNode visit(RelNode other) {
    // Recurse into children first so each child's row type is fully normalized; then re-align
    // the parent's RexNodes (RexCall return types AND RexInputRef stored types) against the new
    // input schema BEFORE invoking the parent's copy(). Going through super.visit() would call
    // parent.copy(traitSet, inputs) right after each child swap, firing the parent's
    // constructor assertion (ref:EXPR_TIMESTAMP VARCHAR vs input:TIMESTAMP(9)) before we get to
    // patch the stale RexInputRefs.
    List<RelNode> normalizedChildren = recurseChildren(other);
    boolean inputsChanged = false;
    for (int i = 0; i < normalizedChildren.size(); i++) {
      if (normalizedChildren.get(i) != other.getInputs().get(i)) {
        inputsChanged = true;
        break;
      }
    }
    RexBuilder rexBuilder = other.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    List<RelDataType> inputFieldTypes = concatFieldTypes(normalizedChildren);
    NormalizeShuttle shuttle = new NormalizeShuttle(typeFactory, inputFieldTypes);
    return rebuild(other, normalizedChildren, inputsChanged, shuttle);
  }

  /** Recurse into each child via {@link #visit(RelNode)}, returning the (possibly new) children. */
  private List<RelNode> recurseChildren(RelNode rel) {
    java.util.ArrayList<RelNode> out = new java.util.ArrayList<>(rel.getInputs().size());
    stack.push(rel);
    try {
      for (RelNode input : rel.getInputs()) {
        out.add(input.accept(this));
      }
    } finally {
      stack.pop();
    }
    return out;
  }

  /**
   * Concatenated field types of all children, in input-index order; matches RexInputRef indexing.
   */
  private static List<RelDataType> concatFieldTypes(List<RelNode> children) {
    java.util.ArrayList<RelDataType> out = new java.util.ArrayList<>();
    for (RelNode child : children) {
      for (RelDataTypeField f : child.getRowType().getFieldList()) {
        out.add(f.getType());
      }
    }
    return out;
  }

  /**
   * Reassemble {@code rel} with normalized children + RexShuttle-rewritten RexNodes. We dispatch
   * per-RelNode type to the {@code copy(traits, input, ...rex)} variant that takes both new input
   * and new rex args together — using {@code copy(traits, inputs)} would copy the original (stale)
   * RexNodes with the new input, firing the parent's constructor assertion.
   */
  private static RelNode rebuild(
      RelNode rel, List<RelNode> children, boolean inputsChanged, NormalizeShuttle shuttle) {
    if (rel instanceof Project project) {
      List<RexNode> rewrittenExps = shuttle.apply(project.getProjects());
      boolean expsChanged = rewrittenExps != project.getProjects();
      if (!inputsChanged && !expsChanged) {
        return project;
      }
      RelDataType newRowType =
          RexUtil.createStructType(
              shuttle.typeFactory, rewrittenExps, project.getRowType().getFieldNames(), null);
      return project.copy(project.getTraitSet(), children.get(0), rewrittenExps, newRowType);
    }
    if (rel instanceof Filter filter) {
      RexNode rewrittenCondition = filter.getCondition().accept(shuttle);
      boolean conditionChanged = rewrittenCondition != filter.getCondition();
      if (!inputsChanged && !conditionChanged) {
        return filter;
      }
      return filter.copy(filter.getTraitSet(), children.get(0), rewrittenCondition);
    }
    if (rel instanceof Join join) {
      RexNode rewrittenCondition = join.getCondition().accept(shuttle);
      boolean conditionChanged = rewrittenCondition != join.getCondition();
      if (!inputsChanged && !conditionChanged) {
        return join;
      }
      return join.copy(
          join.getTraitSet(),
          rewrittenCondition,
          children.get(0),
          children.get(1),
          join.getJoinType(),
          join.isSemiJoinDone());
    }
    // Aggregate, Sort, TableScan, Union, etc.: row-type-stable nodes (no RexNodes carrying stale
    // input refs in a way that requires per-type rebuild) — accept(RexShuttle) handles their
    // RexNode payloads (e.g. Sort collations) and we plug the new inputs via copy(traits, inputs).
    RelNode withRex = rel.accept(shuttle);
    if (!inputsChanged) {
      return withRex;
    }
    return withRex.copy(withRex.getTraitSet(), children);
  }

  /** Rewrites UDT return types on calls and stale UDT types on input refs to the standard type. */
  private static final class NormalizeShuttle extends RexShuttle {
    private final RelDataTypeFactory typeFactory;
    private final List<RelDataType> inputFieldTypes;

    NormalizeShuttle(RelDataTypeFactory typeFactory, List<RelDataType> inputFieldTypes) {
      this.typeFactory = typeFactory;
      this.inputFieldTypes = inputFieldTypes;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      call = (RexCall) super.visitCall(call);
      Optional<UdtMapping> mapping = UdtMapping.fromUdtType(call.getType());
      if (mapping.isEmpty()) {
        return call;
      }
      return call.clone(toStdType(call.getType(), mapping.get()), call.getOperands());
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      // Re-align stored type against the post-normalization input field type at this index.
      int index = inputRef.getIndex();
      if (index < 0 || index >= inputFieldTypes.size()) {
        return inputRef;
      }
      RelDataType actual = inputFieldTypes.get(index);
      if (actual.equals(inputRef.getType())) {
        return inputRef;
      }
      return new RexInputRef(index, actual);
    }

    private RelDataType toStdType(RelDataType original, UdtMapping mapping) {
      SqlTypeName stdTypeName = mapping.getStdType();
      RelDataType baseType =
          stdTypeName.allowsPrec()
              ? typeFactory.createSqlType(
                  stdTypeName, typeFactory.getTypeSystem().getMaxPrecision(stdTypeName))
              : typeFactory.createSqlType(stdTypeName);
      return typeFactory.createTypeWithNullability(baseType, original.isNullable());
    }
  }
}

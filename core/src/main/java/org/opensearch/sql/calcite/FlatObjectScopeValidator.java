/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.QueryProcessingStage;
import org.opensearch.sql.common.utils.StringUtils;

/**
 * Keeps a query to what a {@code flat_object} field can answer.
 *
 * <p>A flat_object indexes every leaf value as a keyword term with the path folded in and gives the
 * leaf no mapping of its own, so OpenSearch can look a leaf up but has nothing to compare, sort or
 * aggregate it by. PPL supports every lookup the index answers -- projecting a leaf, and filtering
 * it by value, by existence or by a pattern, which are pushed down as term, terms, exists, prefix
 * and wildcard queries -- and rejects the rest here, while the plan is built, rather than reading
 * every record to answer it.
 *
 * <p>A comparison is rejected rather than pushed down because the index would answer it, but
 * wrongly: the terms are text, so {@code > 90} does not match the leaf that holds 200.
 *
 * <p>Both are recognized by type: a flat_object field is {@code MAP<VARCHAR, VARIANT>} and a leaf
 * of one is {@code VARIANT}, the types the field is registered with.
 */
public final class FlatObjectScopeValidator {

  private static final String DOC_URL =
      "https://docs.opensearch.org/latest/mappings/supported-field-types/flat-object/";

  private static final String SUGGESTION =
      "A flat_object field can only be projected, or looked up in the index -- by value, by"
          + " existence or by a pattern. See "
          + DOC_URL;

  private FlatObjectScopeValidator() {}

  /** Throws if the plan uses a flat_object field beyond what its index can answer. */
  public static void validate(RelNode plan) {
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        check(node);
        super.visit(node, ordinal, parent);
      }
    }.go(plan);
  }

  /**
   * The error for a function that cannot take a flat_object leaf, raised by the function resolver
   * in place of its own type error so that the reason is the same wherever it surfaces.
   */
  public static ErrorReport unsupportedFunction(String functionName) {
    return unsupported("Cannot apply %s to a flat_object leaf", functionName);
  }

  private static void check(RelNode node) {
    List<RelDataTypeField> inputFields = inputFields(node);
    // Sort keys, group keys, aggregate arguments and the column an expand correlates on are
    // positions, not expressions, so they are checked against the input row type directly.
    if (node instanceof Sort sort) {
      for (var key : sort.getCollation().getFieldCollations()) {
        rejectIfFlatObject(node, inputFields, key.getFieldIndex(), "Cannot sort by %s");
      }
      return;
    }
    if (node instanceof Aggregate aggregate) {
      for (int key : aggregate.getGroupSet()) {
        rejectIfFlatObject(node, inputFields, key, "Cannot group by %s");
      }
      for (AggregateCall call : aggregate.getAggCallList()) {
        for (int arg : call.getArgList()) {
          rejectIfFlatObject(
              node,
              inputFields,
              arg,
              "Cannot apply "
                  + call.getAggregation().getName().toLowerCase(Locale.ROOT)
                  + " to %s");
        }
      }
      return;
    }
    if (node instanceof Correlate correlate) {
      for (int required : correlate.getRequiredColumns()) {
        rejectIfFlatObject(node, inputFields, required, "Cannot expand %s");
      }
      return;
    }
    if (node instanceof Project project) {
      // A leaf may be projected as it is; an expression over one -- a cast included -- is computed
      // from the value rather than looked up in the index, which is the whole of what this type
      // supports.
      for (RexNode expression : project.getProjects()) {
        if (isLeafRef(expression, inputFields)
            && !(expression instanceof RexCall call && call.getKind() != SqlKind.ITEM)) {
          continue;
        }
        List<String> leaves = leavesIn(expression, inputFields);
        if (!leaves.isEmpty()) {
          throw unsupported("Cannot evaluate an expression over %s", String.join(", ", leaves));
        }
      }
      return;
    }
    if (node instanceof Filter filter && filter.getCondition() instanceof RexCall condition) {
      checkPredicate(condition, inputFields);
      return;
    }
    // Any other node -- a join condition, a window, a command with expressions of its own -- may
    // pass a leaf through by position, but nothing it computes may read one.
    node.accept(
        new RexShuttle() {
          @Override
          public RexNode visitInputRef(RexInputRef ref) {
            if (isFlatObjectOrLeaf(inputFields.get(ref.getIndex()).getType())) {
              throw unsupported("Cannot use %s here", name(inputFields, ref.getIndex()));
            }
            return ref;
          }
        });
  }

  /** The fields a {@link RexInputRef} inside {@code node} addresses: its inputs, in order. */
  private static List<RelDataTypeField> inputFields(RelNode node) {
    List<RelDataTypeField> fields = new ArrayList<>();
    for (RelNode input : node.getInputs()) {
      fields.addAll(input.getRowType().getFieldList());
    }
    return fields;
  }

  static boolean isFlatObject(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.MAP
        && type.getValueType() != null
        && type.getValueType().getSqlTypeName() == SqlTypeName.VARIANT;
  }

  private static boolean isFlatObjectOrLeaf(RelDataType type) {
    return isFlatObject(type) || type.getSqlTypeName() == SqlTypeName.VARIANT;
  }

  /** A leaf reference: {@code ITEM(<flat_object field>, '<path>')}, or a column that is one. */
  private static boolean isLeafRef(RexNode node, List<RelDataTypeField> inputFields) {
    node = PlanUtils.stripCast(node);
    if (node instanceof RexInputRef ref) {
      return isFlatObjectOrLeaf(inputFields.get(ref.getIndex()).getType());
    }
    return node instanceof RexCall call
        && call.getKind() == SqlKind.ITEM
        && call.getOperands().get(0) instanceof RexInputRef ref
        && isFlatObject(ref.getType())
        && call.getOperands().get(1) instanceof RexLiteral;
  }

  /** {@code field.path} for a leaf reference, as a query author writes it. */
  private static String leafName(RexNode node, List<RelDataTypeField> inputFields) {
    node = PlanUtils.stripCast(node);
    if (node instanceof RexInputRef ref) {
      return name(inputFields, ref.getIndex());
    }
    RexCall item = (RexCall) node;
    return name(inputFields, ((RexInputRef) item.getOperands().get(0)).getIndex())
        + "."
        + RexLiteral.stringValue((RexLiteral) item.getOperands().get(1));
  }

  /** Throws unless the filter is one the index answers on its own, or touches no leaf at all. */
  private static void checkPredicate(RexCall call, List<RelDataTypeField> inputFields) {
    List<String> leaves = leavesIn(call, inputFields);
    if (leaves.isEmpty()) {
      return;
    }
    switch (call.getKind()) {
      case AND, OR, NOT -> {
        call.getOperands().stream()
            .filter(RexCall.class::isInstance)
            .forEach(operand -> checkPredicate((RexCall) operand, inputFields));
        return;
      }
      case EQUALS, NOT_EQUALS -> {
        // Either value the index holds under the term is the one the query asked for, whichever
        // way the literal is written, so a term query answers it as it answers the same DSL query.
        RexNode left = PlanUtils.stripCast(call.getOperands().get(0));
        RexNode right = PlanUtils.stripCast(call.getOperands().get(1));
        if ((isLeafRef(left, inputFields)
                && PlanUtils.stripCastOfLiteral(right) instanceof RexLiteral)
            || (isLeafRef(right, inputFields)
                && PlanUtils.stripCastOfLiteral(left) instanceof RexLiteral)) {
          return;
        }
      }
      case IS_NULL, IS_NOT_NULL -> {
        if (isLeafRef(call.getOperands().get(0), inputFields)) {
          return;
        }
      }
      case SEARCH -> {
        // Calcite folds `x = 'a' or x = 'b'` and `x in (...)` into a SEARCH over a set of points,
        // and `x not in (...)` into its complement; the index answers either with one terms query.
        if (isLeafRef(call.getOperands().get(0), inputFields)
            && call.getOperands().get(1) instanceof RexLiteral literal
            && literal.getValueAs(Sarg.class) instanceof Sarg<?> sarg) {
          if (sarg.isPoints() || sarg.isComplementedPoints()) {
            return;
          }
          // A range Sarg is `between`, or comparisons Calcite folded together. Neither operator
          // survives the fold, so the message names only the field; "search" is Calcite's name
          // for the folded call and means nothing to whoever wrote the query.
          throw cannotCompare(leaves, null);
        }
      }
      case LIKE -> {
        // LIKE and ILIKE both: a leading prefix is a prefix query and any other pattern a wildcard
        // query, matched case-insensitively when the operator is ILIKE.
        if (call.getOperator() instanceof SqlLikeOperator
            && isLeafRef(call.getOperands().get(0), inputFields)
            && isTextLiteral(call.getOperands().get(1))) {
          return;
        }
      }
      case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, BETWEEN -> {
        throw cannotCompare(leaves, call.getOperator().getName().toLowerCase(Locale.ROOT));
      }
      default -> {}
    }
    throw unsupported(
        "Cannot filter %s with %s",
        String.join(", ", leaves), call.getOperator().getName().toLowerCase(Locale.ROOT));
  }

  /** The leaf references inside an expression, as a query author writes them. */
  private static List<String> leavesIn(RexNode node, List<RelDataTypeField> inputFields) {
    // a set: a leaf referenced twice in one expression is still one field to name
    Set<String> leaves = new LinkedHashSet<>();
    node.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef ref) {
            if (isFlatObjectOrLeaf(inputFields.get(ref.getIndex()).getType())) {
              leaves.add(name(inputFields, ref.getIndex()));
            }
            return null;
          }

          @Override
          public Void visitCall(RexCall call) {
            if (isLeafRef(call, inputFields)) {
              leaves.add(leafName(call, inputFields));
              return null;
            }
            return super.visitCall(call);
          }
        });
    return List.copyOf(leaves);
  }

  private static void rejectIfFlatObject(
      RelNode node, List<RelDataTypeField> inputFields, int index, String message) {
    if (index < inputFields.size() && isFlatObjectOrLeaf(inputFields.get(index).getType())) {
      throw unsupported(message, displayName(node, inputFields, index));
    }
  }

  private static String name(List<RelDataTypeField> inputFields, int index) {
    return StringUtils.unquoteIdentifier(inputFields.get(index).getName());
  }

  /**
   * The name to show for an input column. A command that sorts or groups by a leaf projects it into
   * a column of its own first, and that column has a generated name ({@code $f2}); name the leaf
   * that projection reads instead, which is what the query says.
   */
  private static String displayName(RelNode node, List<RelDataTypeField> inputFields, int index) {
    if (!node.getInputs().isEmpty() && node.getInput(0) instanceof Project project) {
      List<RelDataTypeField> below = inputFields(project);
      RexNode expression = project.getProjects().get(index);
      if (isLeafRef(expression, below)) {
        return leafName(expression, below);
      }
    }
    return name(inputFields, index);
  }

  private static boolean isTextLiteral(RexNode node) {
    return node instanceof RexLiteral literal && SqlTypeUtil.isCharacter(literal.getType());
  }

  /**
   * A comparison is the one predicate the index would answer but answer wrongly: the terms are text
   * and the leaf has no mapping, so a range query over them orders every value as text. The message
   * carries only the field and the operator, both read off the query; the operator is absent when
   * Calcite folded the comparison into a Sarg, which keeps neither side.
   */
  private static ErrorReport cannotCompare(List<String> leaves, @Nullable String operator) {
    return unsupported(
        "Cannot compare %s",
        String.join(", ", leaves) + (operator == null ? "" : " with " + operator));
  }

  /**
   * The rule goes in the suggestion and the field in the context, as {@code
   * AnalyticsEngineFormatSupport} and the alias check in {@code OpenSearchDataType} raise an
   * unsupported operation; the cause message says only what this query did.
   */
  private static ErrorReport unsupported(String message, Object... args) {
    return ErrorReport.wrap(new IllegalArgumentException(StringUtils.format(message, args)))
        .code(ErrorCode.UNSUPPORTED_OPERATION)
        .stage(QueryProcessingStage.ANALYZING)
        .location("while checking what a flat_object field can answer")
        .suggestion(SUGGESTION)
        .build();
  }
}

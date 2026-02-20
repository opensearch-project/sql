/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.apache.calcite.rex.RexWindowBounds.CURRENT_ROW;
import static org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_FOLLOWING;
import static org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_PRECEDING;
import static org.apache.calcite.rex.RexWindowBounds.following;
import static org.apache.calcite.rex.RexWindowBounds.preceding;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.isTimeBasedType;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.WindowBound;
import org.opensearch.sql.ast.expression.WindowFrame;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

public interface PlanUtils {

  /** this is only for dedup command, do not reuse it in other command */
  String ROW_NUMBER_COLUMN_FOR_DEDUP = "_row_number_dedup_";

  String ROW_NUMBER_COLUMN_FOR_RARE_TOP = "_row_number_rare_top_";
  String ROW_NUMBER_COLUMN_FOR_MAIN = "_row_number_main_";
  String ROW_NUMBER_COLUMN_FOR_SUBSEARCH = "_row_number_subsearch_";
  String ROW_NUMBER_COLUMN_FOR_STREAMSTATS = "__stream_seq__";
  String ROW_NUMBER_COLUMN_FOR_CHART = "_row_number_chart_";
  String ROW_NUMBER_COLUMN_FOR_TRANSPOSE = "_row_number_transpose_";

  static SpanUnit intervalUnitToSpanUnit(IntervalUnit unit) {
    return switch (unit) {
      case MICROSECOND -> SpanUnit.MICROSECOND;
      case MILLISECOND -> SpanUnit.MILLISECOND;
      case SECOND -> SpanUnit.SECOND;
      case MINUTE -> SpanUnit.MINUTE;
      case HOUR -> SpanUnit.HOUR;
      case DAY -> SpanUnit.DAY;
      case WEEK -> SpanUnit.WEEK;
      case MONTH -> SpanUnit.MONTH;
      case QUARTER -> SpanUnit.QUARTER;
      case YEAR -> SpanUnit.YEAR;
      case UNKNOWN -> SpanUnit.UNKNOWN;
      default -> throw new UnsupportedOperationException("Unsupported interval unit: " + unit);
    };
  }

  static IntervalUnit spanUnitToIntervalUnit(SpanUnit unit) {
    switch (unit) {
      case MICROSECOND:
      case US:
        return IntervalUnit.MICROSECOND;
      case MILLISECOND:
      case MS:
        return IntervalUnit.MILLISECOND;
      case SECOND:
      case SECONDS:
      case SEC:
      case SECS:
      case S:
        return IntervalUnit.SECOND;
      case MINUTE:
      case MINUTES:
      case MIN:
      case MINS:
      case m:
        return IntervalUnit.MINUTE;
      case HOUR:
      case HOURS:
      case HR:
      case HRS:
      case H:
        return IntervalUnit.HOUR;
      case DAY:
      case DAYS:
      case D:
        return IntervalUnit.DAY;
      case WEEK:
      case WEEKS:
      case W:
        return IntervalUnit.WEEK;
      case MONTH:
      case MONTHS:
      case MON:
      case M:
        return IntervalUnit.MONTH;
      case QUARTER:
      case QUARTERS:
      case QTR:
      case QTRS:
      case Q:
        return IntervalUnit.QUARTER;
      case YEAR:
      case YEARS:
      case Y:
        return IntervalUnit.YEAR;
      case UNKNOWN:
        return IntervalUnit.UNKNOWN;
      default:
        throw new UnsupportedOperationException("Unsupported span unit: " + unit);
    }
  }

  static RexNode makeOver(
      CalcitePlanContext context,
      BuiltinFunctionName functionName,
      RexNode field,
      List<RexNode> argList,
      List<RexNode> partitions,
      List<RexNode> orderKeys,
      @Nullable WindowFrame windowFrame) {
    if (windowFrame == null) {
      windowFrame = WindowFrame.rowsUnbounded();
    }
    boolean rows = windowFrame.getType() == WindowFrame.FrameType.ROWS;
    RexWindowBound lowerBound = convert(context, windowFrame.getLower());
    RexWindowBound upperBound = convert(context, windowFrame.getUpper());
    switch (functionName) {
      // There is no "avg" AggImplementor in Calcite, we have to change avg window
      // function to `sum over(...).toRex / count over(...).toRex`
      case AVG:
        // avg(x) ==>
        //     sum(x) / count(x)
        return context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE,
            sumOver(context, field, partitions, rows, lowerBound, upperBound),
            context.relBuilder.cast(
                countOver(context, field, partitions, rows, lowerBound, upperBound),
                SqlTypeName.DOUBLE));
      // stddev_pop(x) ==>
      //     power((sum(x * x) - sum(x) * sum(x) / count(x)) / count(x), 0.5)
      //
      // stddev_samp(x) ==>
      //     power((sum(x * x) - sum(x) * sum(x) / count(x)) / (count(x) - 1), 0.5)
      //
      // var_pop(x) ==>
      //     (sum(x * x) - sum(x) * sum(x) / count(x)) / count(x)
      //
      // var_samp(x) ==>
      //     (sum(x * x) - sum(x) * sum(x) / count(x)) / (count(x) - 1)
      case STDDEV_POP:
        return variance(context, field, partitions, rows, lowerBound, upperBound, true, true);
      case STDDEV_SAMP:
        return variance(context, field, partitions, rows, lowerBound, upperBound, false, true);
      case VARPOP:
        return variance(context, field, partitions, rows, lowerBound, upperBound, true, false);
      case VARSAMP:
        return variance(context, field, partitions, rows, lowerBound, upperBound, false, false);
      case ROW_NUMBER:
        return withOver(
            context.relBuilder.aggregateCall(SqlStdOperatorTable.ROW_NUMBER),
            partitions,
            orderKeys,
            true,
            lowerBound,
            upperBound);
      case NTH_VALUE:
        return withOver(
            context.relBuilder.aggregateCall(SqlStdOperatorTable.NTH_VALUE, field, argList.get(0)),
            partitions,
            orderKeys,
            true,
            lowerBound,
            upperBound);
      default:
        return withOver(
            makeAggCall(context, functionName, false, field, argList),
            partitions,
            orderKeys,
            rows,
            lowerBound,
            upperBound);
    }
  }

  private static RexNode sumOver(
      CalcitePlanContext ctx,
      RexNode operation,
      List<RexNode> partitions,
      boolean rows,
      RexWindowBound lowerBound,
      RexWindowBound upperBound) {
    return withOver(
        ctx.relBuilder.sum(operation), partitions, List.of(), rows, lowerBound, upperBound);
  }

  private static RexNode countOver(
      CalcitePlanContext ctx,
      RexNode operation,
      List<RexNode> partitions,
      boolean rows,
      RexWindowBound lowerBound,
      RexWindowBound upperBound) {
    return withOver(
        ctx.relBuilder.count(ImmutableList.of(operation)),
        partitions,
        List.of(),
        rows,
        lowerBound,
        upperBound);
  }

  private static RexNode withOver(
      RelBuilder.AggCall aggCall,
      List<RexNode> partitions,
      List<RexNode> orderKeys,
      boolean rows,
      RexWindowBound lowerBound,
      RexWindowBound upperBound) {
    return aggCall
        .over()
        .partitionBy(partitions)
        .orderBy(orderKeys)
        .let(
            c ->
                rows
                    ? c.rowsBetween(lowerBound, upperBound)
                    : c.rangeBetween(lowerBound, upperBound))
        .toRex();
  }

  private static RexNode variance(
      CalcitePlanContext ctx,
      RexNode operator,
      List<RexNode> partitions,
      boolean rows,
      RexWindowBound lowerBound,
      RexWindowBound upperBound,
      boolean biased,
      boolean sqrt) {
    RexNode argSquared = ctx.relBuilder.call(SqlStdOperatorTable.MULTIPLY, operator, operator);
    RexNode sumArgSquared = sumOver(ctx, argSquared, partitions, rows, lowerBound, upperBound);
    RexNode sum = sumOver(ctx, operator, partitions, rows, lowerBound, upperBound);
    RexNode sumSquared = ctx.relBuilder.call(SqlStdOperatorTable.MULTIPLY, sum, sum);
    RexNode count = countOver(ctx, operator, partitions, rows, lowerBound, upperBound);
    RexNode countCast = ctx.relBuilder.cast(count, SqlTypeName.DOUBLE);
    RexNode avgSumSquared = ctx.relBuilder.call(SqlStdOperatorTable.DIVIDE, sumSquared, countCast);
    RexNode diff = ctx.relBuilder.call(SqlStdOperatorTable.MINUS, sumArgSquared, avgSumSquared);
    RexNode denominator;
    if (biased) {
      denominator = countCast;
    } else {
      RexNode one = ctx.relBuilder.literal(1);
      denominator = ctx.relBuilder.call(SqlStdOperatorTable.MINUS, countCast, one);
    }
    RexNode div = ctx.relBuilder.call(SqlStdOperatorTable.DIVIDE, diff, denominator);
    RexNode result = div;
    if (sqrt) {
      RexNode half = ctx.relBuilder.literal(0.5);
      result = ctx.relBuilder.call(SqlStdOperatorTable.POWER, div, half);
    }
    return result;
  }

  static RexWindowBound convert(CalcitePlanContext context, WindowBound windowBound) {
    if (windowBound instanceof WindowBound.UnboundedWindowBound unbounded) {
      if (unbounded.isPreceding()) {
        return UNBOUNDED_PRECEDING;
      } else {
        return UNBOUNDED_FOLLOWING;
      }
    } else if (windowBound instanceof WindowBound.CurrentRowWindowBound current) {
      return CURRENT_ROW;
    } else if (windowBound instanceof WindowBound.OffSetWindowBound offset) {
      if (offset.isPreceding()) {
        return preceding(context.relBuilder.literal(offset.getOffset()));
      } else {
        return following(context.relBuilder.literal(offset.getOffset()));
      }
    } else {
      throw new UnsupportedOperationException("Unexpected window bound: " + windowBound);
    }
  }

  static RelBuilder.AggCall makeAggCall(
      CalcitePlanContext context,
      BuiltinFunctionName functionName,
      boolean distinct,
      RexNode field,
      List<RexNode> argList) {
    return PPLFuncImpTable.INSTANCE.resolveAgg(functionName, distinct, field, argList, context);
  }

  /** Get all uniq input references from a RexNode. */
  static List<RexInputRef> getInputRefs(RexNode node) {
    if (node == null) {
      return List.of();
    }
    List<RexInputRef> inputRefs = new ArrayList<>();
    node.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            if (!inputRefs.contains(inputRef)) {
              inputRefs.add(inputRef);
            }
            return null;
          }
        });
    return inputRefs;
  }

  /** Get all uniq input references from a list of RexNodes. */
  static List<RexInputRef> getInputRefs(List<RexNode> nodes) {
    return nodes.stream().flatMap(node -> getInputRefs(node).stream()).toList();
  }

  /** Get all uniq RexCall from RexNode with a predicate */
  static List<RexCall> getRexCall(RexNode node, Predicate<RexCall> predicate) {
    List<RexCall> list = new ArrayList<>();
    node.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall inputCall) {
            if (predicate.test(inputCall)) {
              if (!list.contains(inputCall)) {
                list.add(inputCall);
              }
            } else {
              inputCall.getOperands().forEach(call -> call.accept(this));
            }
            return null;
          }
        });
    return list;
  }

  /** Get all uniq input references from a list of agg calls. */
  static List<RexInputRef> getInputRefsFromAggCall(List<RelBuilder.AggCall> aggCalls) {
    return aggCalls.stream()
        .map(RelBuilder.AggCall::over)
        .map(RelBuilder.OverCall::toRex)
        .flatMap(rex -> getInputRefs(rex).stream())
        .toList();
  }

  /**
   * Visit the children of an unresolved plan to find it leaf relation
   *
   * @param node to visit its children
   * @return the relation if found
   */
  static UnresolvedPlan getRelation(UnresolvedPlan node) {
    AbstractNodeVisitor<Relation, Object> relationVisitor =
        new AbstractNodeVisitor<Relation, Object>() {
          @Override
          public Relation visitRelation(Relation node, Object context) {
            return node;
          }
        };
    return node.getChild().getFirst().accept(relationVisitor, null);
  }

  /** Similar to {@link org.apache.calcite.plan.RelOptUtil#findTable(RelNode, String) } */
  static RelOptTable findTable(RelNode root) {
    try {
      RelShuttle visitor =
          new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(TableScan scan) {
              final RelOptTable scanTable = scan.getTable();
              throw new Util.FoundOne(scanTable);
            }
          };
      root.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RelOptTable) e.getNode();
    }
  }

  /**
   * Transform plan to attach specified child to the first leaf node.
   *
   * @param node to transform
   * @param child to attach
   */
  static void transformPlanToAttachChild(UnresolvedPlan node, UnresolvedPlan child) {
    AbstractNodeVisitor<Void, Object> leafVisitor =
        new AbstractNodeVisitor<Void, Object>() {
          @Override
          public Void visitChildren(Node node, Object context) {
            if (node.getChild() == null || node.getChild().isEmpty()) {
              // find leaf node
              ((UnresolvedPlan) node).attach(child);
            } else {
              node.getChild().forEach(child -> child.accept(this, context));
            }
            return null;
          }
        };
    node.accept(leafVisitor, null);
  }

  /**
   * Return the first value RexNode of the valid map RexCall structure
   *
   * @param rexNode RexNode that expects type of MAP_VALUE_CONSTRUCTOR RexCall
   * @return first value of the valid map RexCall
   */
  static RexNode derefMapCall(RexNode rexNode) {
    if (rexNode instanceof RexCall) {
      RexCall call = (RexCall) rexNode;
      if (call.getOperator() == SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR) {
        return call.getOperands().get(1);
      }
    }
    return rexNode;
  }

  /** Check if contains dedup, it should be put in the last position */
  static boolean containsRowNumberDedup(RelNode node) {
    List<String> fieldNames = node.getRowType().getFieldNames();
    return fieldNames.get(fieldNames.size() - 1).equals(ROW_NUMBER_COLUMN_FOR_DEDUP);
  }

  /** Check if contains dedup for top/rare */
  static boolean containsRowNumberRareTop(RelNode node) {
    return node.getRowType().getFieldNames().stream()
        .anyMatch(ROW_NUMBER_COLUMN_FOR_RARE_TOP::equals);
  }

  /** Get all RexWindow list from LogicalProject */
  static List<RexWindow> getRexWindowFromProject(LogicalProject project) {
    final List<RexWindow> res = new ArrayList<>();
    final RexVisitorImpl<Void> visitor =
        new RexVisitorImpl<>(true) {
          @Override
          public Void visitOver(RexOver over) {
            res.add(over.getWindow());
            return null;
          }
        };
    visitor.visitEach(project.getProjects());
    return res;
  }

  static List<Integer> getSelectColumns(List<RexNode> rexNodes) {
    final List<Integer> selectedColumns = new ArrayList<>();
    final RexVisitorImpl<Void> visitor =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            if (!selectedColumns.contains(inputRef.getIndex())) {
              selectedColumns.add(inputRef.getIndex());
            }
            return null;
          }
        };
    visitor.visitEach(rexNodes);
    return selectedColumns;
  }

  // `RelDecorrelator` may generate a Project with duplicated fields, e.g. Project($0,$0).
  // There will be problem if pushing down the pattern like `Aggregate(AGG($0),{1})-Project($0,$0)`,
  // as it will lead to field-name conflict.
  // We should wait and rely on `AggregateProjectMergeRule` to mitigate it by having this constraint
  // Nevertheless, that rule cannot handle all cases if there is RexCall in the Project,
  // e.g. Project($0, $0, +($0,1)). We cannot push down the Aggregate for this corner case.
  // TODO: Simplify the Project where there is RexCall by adding a new rule.
  static boolean distinctProjectList(LogicalProject project) {
    // Change to Set<Pair<RexNode, String>> to resolve
    // https://github.com/opensearch-project/sql/issues/4347
    Set<Pair<RexNode, String>> rexSet = new HashSet<>();
    return project.getNamedProjects().stream().allMatch(rexSet::add);
  }

  /**
   * The LogicalSort is a LIMIT that should be pushed down when its fetch field is not null and its
   * collation is empty. For example: <code>sort name | head 5</code> should not be pushed down
   * because it has a field collation.
   *
   * @param sort The LogicalSort to check.
   * @return True if the LogicalSort is a LIMIT, false otherwise.
   */
  static boolean isLogicalSortLimit(LogicalSort sort) {
    return sort.fetch != null;
  }

  static boolean containsRexCall(Project project) {
    return project.getProjects().stream().anyMatch(p -> p instanceof RexCall);
  }

  static boolean sortByFieldsOnly(Sort sort) {
    return !sort.getCollation().getFieldCollations().isEmpty() && sort.fetch == null;
  }

  /**
   * Check if the sort collation points to non field project expression.
   *
   * @param sort the sort operator adding sort order over project
   * @param project project operation that may contain non field expressions
   * @return flag to indicate whether non field project expression will be sorted
   */
  static boolean sortReferencesExpr(Sort sort, Project project) {
    if (sort.getCollation().getFieldCollations().isEmpty()) {
      return false;
    }
    return sort.getCollation().getFieldCollations().stream()
        .anyMatch(
            relFieldCollation ->
                project.getProjects().get(relFieldCollation.getFieldIndex()) instanceof RexCall);
  }

  /**
   * Get a string representation of the argument types expressed in ExprType for error messages.
   *
   * @param argTypes the list of argument types as {@link RelDataType}
   * @return a string in the format [type1,type2,...] representing the argument types
   */
  static String getActualSignature(List<RelDataType> argTypes) {
    return "["
        + argTypes.stream()
            .map(OpenSearchTypeFactory::convertRelDataTypeToExprType)
            .map(Objects::toString)
            .collect(Collectors.joining(","))
        + "]";
  }

  /**
   * Check if the RexNode contains any CorrelVariable.
   *
   * @param node the RexNode to check
   * @return true if the RexNode contains any CorrelVariable, false otherwise
   */
  static boolean containsCorrelVariable(RexNode node) {
    try {
      node.accept(
          new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitCorrelVariable(RexCorrelVariable correlVar) {
              throw new RuntimeException("Correl found");
            }
          });
      return false;
    } catch (Exception e) {
      return true;
    }
  }

  /**
   * Reverses the direction of a RelCollation.
   *
   * @param original The original collation to reverse
   * @return A new RelCollation with reversed directions
   */
  public static RelCollation reverseCollation(RelCollation original) {
    if (original == null || original.getFieldCollations().isEmpty()) {
      return original;
    }

    List<RelFieldCollation> reversedFields = new ArrayList<>();
    for (RelFieldCollation field : original.getFieldCollations()) {
      RelFieldCollation.Direction reversedDirection = field.direction.reverse();

      // Handle null direction properly - reverse it as well
      RelFieldCollation.NullDirection reversedNullDirection =
          field.nullDirection == RelFieldCollation.NullDirection.FIRST
              ? RelFieldCollation.NullDirection.LAST
              : field.nullDirection == RelFieldCollation.NullDirection.LAST
                  ? RelFieldCollation.NullDirection.FIRST
                  : field.nullDirection;

      RelFieldCollation reversedField =
          new RelFieldCollation(field.getFieldIndex(), reversedDirection, reversedNullDirection);
      reversedFields.add(reversedField);
    }

    return RelCollations.of(reversedFields);
  }

  /** Adds a rel node to the top of the stack while preserving the field names and aliases. */
  static void replaceTop(RelBuilder relBuilder, RelNode relNode) {
    try {
      Method method = RelBuilder.class.getDeclaredMethod("replaceTop", RelNode.class);
      method.setAccessible(true);
      method.invoke(relBuilder, relNode);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to invoke RelBuilder.replaceTop", e);
    }
  }

  /** Extract the RexLiteral from the aggregate call if the aggregate call is a LITERAL_AGG. */
  static @Nullable RexLiteral getObjectFromLiteralAgg(AggregateCall aggCall) {
    if (aggCall.getAggregation().kind == SqlKind.LITERAL_AGG) {
      return (RexLiteral)
          aggCall.rexList.stream().filter(rex -> rex instanceof RexLiteral).findAny().orElse(null);
    } else {
      return null;
    }
  }

  /**
   * This is a helper method to create a target mapping easily for replacing calling {@link
   * Mappings#target(List, int)}
   *
   * @param rexNodes the rex list in schema
   * @param schema the schema which contains the rex list
   * @return the target mapping
   */
  static Mapping mapping(List<RexNode> rexNodes, RelDataType schema) {
    return Mappings.target(getSelectColumns(rexNodes), schema.getFieldCount());
  }

  static boolean mayBeFilterFromBucketNonNull(LogicalFilter filter) {
    RexNode condition = filter.getCondition();
    return isNotNullOnRef(condition)
        || (condition instanceof RexCall rexCall
            && rexCall.getOperator().equals(SqlStdOperatorTable.AND)
            && rexCall.getOperands().stream().allMatch(PlanUtils::isNotNullOnRef));
  }

  private static boolean isNotNullOnRef(RexNode rex) {
    return rex instanceof RexCall rexCall
        && rexCall.isA(SqlKind.IS_NOT_NULL)
        && rexCall.getOperands().get(0) instanceof RexInputRef;
  }

  Predicate<Aggregate> aggIgnoreNullBucket = PPLHintUtils::ignoreNullBucket;

  Predicate<Aggregate> maybeTimeSpanAgg =
      agg ->
          agg.getGroupSet().stream()
              .allMatch(
                  group ->
                      isTimeBasedType(
                          agg.getInput().getRowType().getFieldList().get(group).getType()));

  static boolean isTimeSpan(RexNode rex) {
    return rex instanceof RexCall rexCall
        && rexCall.getKind() == SqlKind.OTHER_FUNCTION
        && rexCall.getOperator().getName().equalsIgnoreCase(BuiltinFunctionName.SPAN.name())
        && rexCall.getOperands().size() == 3
        && rexCall.getOperands().get(2) instanceof RexLiteral unitLiteral
        && unitLiteral.getTypeName() != SqlTypeName.NULL;
  }

  /**
   * Check if the condition is NOT NULL derived from an aggregate.
   *
   * @param condition the condition to check, composite of single or multiple NOT NULL conditions
   * @param aggregate the aggregate where the condition is derived from
   * @param project the project between the aggregate and the filter
   * @param otherMapping the other mapping generated from ProjectIndexScanRule when applied on the
   *     above project with non-ref expressions.
   * @return true if the condition is single or multiple NOT NULL derived from an aggregate, false
   *     otherwise
   */
  static boolean isNotNullDerivedFromAgg(
      RexNode condition,
      Aggregate aggregate,
      @Nullable Project project,
      @Nullable List<Integer> otherMapping) {
    boolean ignoreNullBucket = aggIgnoreNullBucket.test(aggregate);
    if (!ignoreNullBucket && project == null) return false;
    List<Integer> groupRefList = aggregate.getGroupSet().asList();
    if (project != null) {
      groupRefList =
          groupRefList.stream()
              .map(project.getProjects()::get)
              .filter(rex -> ignoreNullBucket || isTimeSpan(rex))
              .flatMap(expr -> PlanUtils.getInputRefs(expr).stream())
              .map(RexSlot::getIndex)
              .toList();
    }
    if (otherMapping != null) {
      groupRefList = groupRefList.stream().map(otherMapping::get).toList();
    }
    List<Integer> finalGroupRefList = groupRefList;
    Function<RexNode, Boolean> isNotNullFromAgg =
        rex ->
            rex instanceof RexCall rexCall
                && rexCall.isA(SqlKind.IS_NOT_NULL)
                && rexCall.getOperands().get(0) instanceof RexInputRef ref
                && finalGroupRefList.contains(ref.getIndex());
    return isNotNullFromAgg.apply(condition)
        || (condition instanceof RexCall rexCall
            && rexCall.getOperator() == SqlStdOperatorTable.AND
            && rexCall.getOperands().stream().allMatch(isNotNullFromAgg::apply));
  }

  /**
   * Try to prune all RelNodes in the RuleCall from top to down. We can prune a RelNode if:
   *
   * <p>1. It's the root RelNode of the current RuleCall. Or,
   *
   * <p>2. It's logical RelNode and it only has one parent which is pruned. TODO: To be more
   * precisely, we can prun a RelNode whose parents are all pruned, but `prunedNodes` in
   * VolcanoPlanner is not available.
   *
   * @param call the RuleCall to prune
   */
  static void tryPruneRelNodes(RelOptRuleCall call) {
    if (call.getPlanner() instanceof VolcanoPlanner volcanoPlanner) {
      Arrays.stream(call.rels)
          .takeWhile(
              rel ->
                  // Don't prune the physical RelNode as it may prevent sort expr push down
                  rel.getConvention() == Convention.NONE
                      && (rel == call.rels[0]
                          || volcanoPlanner.getSubsetNonNull(rel).getParentRels().size() == 1))
          .forEach(volcanoPlanner::prune);
    }
  }
}

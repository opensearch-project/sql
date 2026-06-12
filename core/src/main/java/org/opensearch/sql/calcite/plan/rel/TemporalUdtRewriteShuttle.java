/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * Walks a RelNode tree and rewrites every standard Calcite DATE/TIME/TIMESTAMP type to its
 * OpenSearch UDT equivalent ({@code ExprDateType}/{@code ExprTimeType}/{@code ExprTimeStampType}).
 * Used at the prepare-statement boundary so analysis sees standard SQL temporal types but physical
 * execution sees UDTs (whose runtime backing is VARCHAR).
 *
 * <p>The shuttle is intended to run <b>before</b> Volcano optimization. After it runs, the RelNode
 * tree is fully UDT-typed and Volcano is free to convert it to EnumerableConvention.
 *
 * <p>Implementation note: Calcite's default {@link
 * org.apache.calcite.rel.RelShuttleImpl#visitChild} rebuilds a parent via {@code
 * parent.copy(traits, newInputs)} AFTER visiting each child. For nodes that store RexNodes
 * (Project, Filter, Calc), this triggers Project/Filter/Calc constructor validation, which compares
 * stored RexInputRef types against the new input row type. Both orderings (rewrite-then-visit and
 * visit-then-rewrite) fail this validation: rewriting parent RexInputRefs first puts UDT refs over
 * a still-standard child; visiting children first puts a standard parent over a UDT child. To
 * handle this, we intercept the visit for those nodes and rebuild them in one shot with both new
 * (rewritten) inputs and rewritten RexNodes.
 */
public class TemporalUdtRewriteShuttle extends RelHomogeneousShuttle {

  private final OpenSearchTypeFactory typeFactory;
  private final TemporalRexShuttle rexShuttle;

  public TemporalUdtRewriteShuttle(OpenSearchTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
    this.rexShuttle = new TemporalRexShuttle(typeFactory, this);
  }

  /** Rewrite a row type by mapping any standard temporal field to its UDT counterpart. */
  public static RelDataType rewriteRowType(OpenSearchTypeFactory tf, RelDataType row) {
    if (!row.isStruct()) {
      return rewriteType(tf, row);
    }
    List<String> names = new ArrayList<>();
    List<RelDataType> types = new ArrayList<>();
    boolean changed = false;
    for (RelDataTypeField f : row.getFieldList()) {
      RelDataType rewritten = rewriteType(tf, f.getType());
      if (rewritten != f.getType()) {
        changed = true;
      }
      names.add(f.getName());
      types.add(rewritten);
    }
    return changed ? tf.createStructType(types, names, row.isNullable()) : row;
  }

  static RelDataType rewriteType(OpenSearchTypeFactory tf, RelDataType t) {
    SqlTypeName name = t.getSqlTypeName();
    boolean nullable = t.isNullable();
    if (name == SqlTypeName.TIMESTAMP) {
      return tf.createUDT(ExprUDT.EXPR_TIMESTAMP, nullable);
    }
    if (name == SqlTypeName.DATE) {
      return tf.createUDT(ExprUDT.EXPR_DATE, nullable);
    }
    if (name == SqlTypeName.TIME) {
      return tf.createUDT(ExprUDT.EXPR_TIME, nullable);
    }
    if (name == SqlTypeName.ARRAY) {
      RelDataType component = t.getComponentType();
      if (component != null) {
        RelDataType rewritten = rewriteType(tf, component);
        if (rewritten != component) {
          return tf.createTypeWithNullability(tf.createArrayType(rewritten, -1), nullable);
        }
      }
    }
    if (name == SqlTypeName.MULTISET) {
      RelDataType component = t.getComponentType();
      if (component != null) {
        RelDataType rewritten = rewriteType(tf, component);
        if (rewritten != component) {
          return tf.createMultisetType(rewritten, -1, nullable);
        }
      }
    }
    if (name == SqlTypeName.MAP) {
      RelDataType key = t.getKeyType();
      RelDataType value = t.getValueType();
      if (key != null && value != null) {
        RelDataType keyR = rewriteType(tf, key);
        RelDataType valueR = rewriteType(tf, value);
        if (keyR != key || valueR != value) {
          return tf.createMapType(keyR, valueR, nullable);
        }
      }
    }
    if (t.isStruct()) {
      return rewriteRowType(tf, t);
    }
    return t;
  }

  /** Rewrite a single RexNode, mapping any standard temporal types to their UDT counterparts. */
  public static RexNode rewriteRex(OpenSearchTypeFactory tf, RexNode node) {
    return node.accept(new TemporalRexShuttle(tf));
  }

  /**
   * Visit children of {@code parent}, returning the rewritten children list. Used to manually walk
   * children when we need to rebuild a parent atomically (i.e. without invoking Calcite's default
   * copy-then-validate path).
   */
  private List<RelNode> rewriteChildren(RelNode parent) {
    List<RelNode> rewritten = new ArrayList<>(parent.getInputs().size());
    for (RelNode child : parent.getInputs()) {
      rewritten.add(child.accept(this));
    }
    return rewritten;
  }

  @Override
  public RelNode visit(RelNode other) {
    // For nodes whose constructor validates RexNodes against input row type (Project, Filter,
    // Calc), we must rebuild the node atomically: new (rewritten) inputs AND rewritten RexNodes
    // must be applied together, so the constructor validation sees a self-consistent picture.
    if (other instanceof Project project) {
      return rebuildProject(project);
    }
    if (other instanceof Filter filter) {
      return rebuildFilter(filter);
    }
    if (other instanceof Calc calc) {
      return rebuildCalc(calc);
    }
    if (other instanceof Aggregate agg) {
      return rebuildAggregate(agg);
    }
    if (other instanceof Values values) {
      return rebuildValues(values);
    }
    if (other instanceof TableScan) {
      return wrapTableScanIfNeeded(other);
    }
    RelNode visited = super.visit(other);
    return visited.accept(rexShuttle);
  }

  private RelNode rebuildProject(Project project) {
    List<RelNode> newInputs = rewriteChildren(project);
    RelNode newInput = newInputs.get(0);
    List<RexNode> rewrittenExps = new ArrayList<>();
    for (RexNode exp : project.getProjects()) {
      rewrittenExps.add(exp.accept(rexShuttle));
    }
    RelDataType outputRowType =
        typeFactory.createStructType(
            rewrittenExps.stream().map(RexNode::getType).toList(),
            project.getRowType().getFieldNames(),
            project.getRowType().isNullable());
    return LogicalProject.create(
        newInput, project.getHints(), rewrittenExps, outputRowType, project.getVariablesSet());
  }

  private RelNode rebuildFilter(Filter filter) {
    List<RelNode> newInputs = rewriteChildren(filter);
    RelNode newInput = newInputs.get(0);
    RexNode rewrittenCondition = filter.getCondition().accept(rexShuttle);
    // Preserve variablesSet so correlated subqueries (Filter referencing $cor0) keep the
    // CorrelationId binding and Calcite doesn't fail with "Correlation variable $cor0 should be
    // defined" during decorrelation/optimization.
    return LogicalFilter.create(
        newInput, rewrittenCondition, ImmutableSet.copyOf(filter.getVariablesSet()));
  }

  private RelNode rebuildCalc(Calc calc) {
    List<RelNode> newInputs = rewriteChildren(calc);
    RelNode newInput = newInputs.get(0);
    RexProgram program = calc.getProgram();
    RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
    List<RexNode> exprs = new ArrayList<>();
    for (RexNode e : program.getExprList()) {
      exprs.add(e.accept(rexShuttle));
    }
    List<RexLocalRef> projects = program.getProjectList();
    RexLocalRef condition = program.getCondition();
    RelDataType rewrittenOutput = rewriteRowType(typeFactory, program.getOutputRowType());
    RexProgram rebuilt =
        RexProgramBuilder.create(
                rexBuilder,
                newInput.getRowType(),
                exprs,
                projects,
                condition,
                rewrittenOutput,
                true,
                null)
            .getProgram(false);
    if (calc instanceof LogicalCalc) {
      return LogicalCalc.create(newInput, rebuilt);
    }
    return calc.copy(calc.getTraitSet(), newInput, rebuilt);
  }

  private RelNode rebuildAggregate(Aggregate agg) {
    List<RelNode> newInputs = rewriteChildren(agg);
    RelNode newInput = newInputs.get(0);
    List<AggregateCall> rewrittenCalls = new ArrayList<>();
    for (AggregateCall call : agg.getAggCallList()) {
      RelDataType target = rewriteType(typeFactory, call.getType());
      // Also walk the post-aggregate reference expressions — they may contain temporal types
      // that need rewriting even when the aggregate's return type does not.
      List<RexNode> rewrittenRexList = new ArrayList<>(call.rexList.size());
      boolean rexListChanged = false;
      for (RexNode r : call.rexList) {
        RexNode rewritten = r.accept(rexShuttle);
        if (rewritten != r) {
          rexListChanged = true;
        }
        rewrittenRexList.add(rewritten);
      }
      if (target == call.getType() && !rexListChanged) {
        rewrittenCalls.add(call);
      } else {
        rewrittenCalls.add(
            AggregateCall.create(
                call.getAggregation(),
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                rewrittenRexList,
                call.getArgList(),
                call.filterArg,
                call.distinctKeys,
                call.collation,
                target,
                call.getName()));
      }
    }
    return agg.copy(
        agg.getTraitSet(), newInput, agg.getGroupSet(), agg.getGroupSets(), rewrittenCalls);
  }

  private RelNode rebuildValues(Values values) {
    RelDataType current = values.getRowType();
    RelDataType rewritten = rewriteRowType(typeFactory, current);
    if (rewritten == current) {
      return values;
    }
    if (values instanceof LogicalValues) {
      // Also rewrite each RexLiteral in every tuple so column types agree with the new row type.
      // Otherwise LogicalValues.create asserts on a std-TIMESTAMP literal under a UDT column.
      ImmutableList.Builder<ImmutableList<RexLiteral>> newTuples = ImmutableList.builder();
      for (ImmutableList<RexLiteral> tuple : values.getTuples()) {
        ImmutableList.Builder<RexLiteral> newTuple = ImmutableList.builder();
        for (RexLiteral lit : tuple) {
          RexNode rewrittenLit = lit.accept(rexShuttle);
          if (rewrittenLit instanceof RexLiteral rewrittenRexLit) {
            newTuple.add(rewrittenRexLit);
          } else {
            // TemporalRexShuttle.visitLiteral wraps in a CAST for UDT-target types, producing a
            // RexCall (not a RexLiteral). LogicalValues can only hold RexLiterals, so we can't
            // use LogicalValues here — fall back to LogicalProject(CAST...) over the values.
            return wrapTableScanIfNeeded(values);
          }
        }
        newTuples.add(newTuple.build());
      }
      return LogicalValues.create(values.getCluster(), rewritten, newTuples.build());
    }
    return wrapTableScanIfNeeded(values);
  }

  private RelNode wrapTableScanIfNeeded(RelNode scan) {
    RelDataType current = scan.getRowType();
    RelDataType rewritten = rewriteRowType(typeFactory, current);
    if (rewritten == current) {
      return scan;
    }
    // Preferred path: rewrite the scan's row type in place so Calcite's Linq4j codegen sees the
    // same UDT (VARCHAR-backed) types that OpenSearchExprValueFactory delivers at runtime.
    if (scan instanceof TemporalSchemaRewritable rewritable) {
      return rewritable.withRowType(rewritten);
    }
    // Fallback for test stubs and non-OpenSearch scans: wrap in a LogicalProject(CAST...).
    RexBuilder rb = scan.getCluster().getRexBuilder();
    List<RexNode> projects = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (int i = 0; i < current.getFieldCount(); i++) {
      RelDataTypeField src = current.getFieldList().get(i);
      RelDataType target = rewritten.getFieldList().get(i).getType();
      RexNode ref = rb.makeInputRef(src.getType(), i);
      if (src.getType() != target) {
        ref = rb.makeCast(target, ref);
      }
      projects.add(ref);
      names.add(src.getName());
    }
    return LogicalProject.create(scan, ImmutableList.of(), projects, names);
  }

  /** Inner shuttle that rewrites RexNode types (input refs, literals, call return types). */
  private static class TemporalRexShuttle extends RexShuttle {
    private final OpenSearchTypeFactory tf;

    /**
     * Reference to the outer RelNode shuttle so we can recurse into the inner plan of {@link
     * RexSubQuery}, which Calcite's default {@link RexShuttle#visitSubQuery} does NOT walk.
     */
    private final TemporalUdtRewriteShuttle relShuttle;

    TemporalRexShuttle(OpenSearchTypeFactory tf) {
      this(tf, null);
    }

    TemporalRexShuttle(OpenSearchTypeFactory tf, TemporalUdtRewriteShuttle relShuttle) {
      this.tf = tf;
      this.relShuttle = relShuttle;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      RelDataType rewritten = rewriteType(tf, inputRef.getType());
      if (rewritten == inputRef.getType()) {
        return inputRef;
      }
      return new RexInputRef(inputRef.getIndex(), rewritten);
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      RelDataType rewritten = rewriteType(tf, literal.getType());
      if (rewritten == literal.getType()) {
        return literal;
      }
      return new RexBuilder(tf).makeCast(rewritten, literal);
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexNode visited = super.visitCall(call);
      if (!(visited instanceof RexCall)) {
        return visited;
      }
      RexCall visitedCall = (RexCall) visited;
      RelDataType rewritten = rewriteType(tf, visitedCall.getType());
      if (rewritten == visitedCall.getType()) {
        return visitedCall;
      }
      return visitedCall.clone(rewritten, visitedCall.getOperands());
    }

    /**
     * Correlated subqueries reference outer columns through {@code $cor0}. If the outer scan's row
     * type gets rewritten to UDT but the correlation variable still carries the standard row type,
     * field access into it yields the wrong type. Rewrite the variable's row type in place.
     */
    @Override
    public RexNode visitCorrelVariable(org.apache.calcite.rex.RexCorrelVariable variable) {
      RelDataType rewritten = rewriteRowType(tf, variable.getType());
      if (rewritten == variable.getType()) {
        return variable;
      }
      return new RexBuilder(tf).makeCorrel(rewritten, variable.id);
    }

    /**
     * Field access through a rewritten row (e.g. {@code $cor0.ts}) needs its cached RexFieldAccess
     * type refreshed. Calcite's default keeps the original type; rebuild by re-reading the field
     * from the (rewritten) reference expression.
     */
    @Override
    public RexNode visitFieldAccess(org.apache.calcite.rex.RexFieldAccess fieldAccess) {
      RexNode refExpr = fieldAccess.getReferenceExpr();
      RexNode rewrittenRef = refExpr.accept(this);
      RelDataType currentFieldType = fieldAccess.getField().getType();
      RelDataType rewrittenFieldType = rewriteType(tf, currentFieldType);
      if (rewrittenRef == refExpr && rewrittenFieldType == currentFieldType) {
        return fieldAccess;
      }
      return new RexBuilder(tf)
          .makeFieldAccess(rewrittenRef, fieldAccess.getField().getName(), true);
    }

    /**
     * Recurse into the inner plan of a correlated/uncorrelated subquery. Calcite's default {@link
     * RexShuttle#visitSubQuery} only walks the operands list and never touches {@code
     * subQuery.rel}, which means scans and predicates inside an EXISTS/IN/SCALAR subquery would
     * keep their original (standard SQL) temporal types. That, in turn, causes pushed-down filter
     * scripts on those subquery scans to be generated against {@code Long}-backed temporal types
     * even though OpenSearch delivers temporal field values as {@code String} at runtime — which
     * surfaces as {@code ClassCastException: String cannot be cast to Long} on the shard.
     */
    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      RexNode rewrittenOperands = super.visitSubQuery(subQuery);
      if (relShuttle == null) {
        return rewrittenOperands;
      }
      RexSubQuery base =
          (rewrittenOperands instanceof RexSubQuery) ? (RexSubQuery) rewrittenOperands : subQuery;
      RelNode rewrittenInner = base.rel.accept(relShuttle);
      if (rewrittenInner == base.rel && rewrittenOperands == subQuery) {
        return subQuery;
      }
      RelDataType rewrittenType = rewriteType(tf, base.getType());
      RexSubQuery clone = base.clone(rewrittenInner);
      if (rewrittenType != base.getType()) {
        clone = (RexSubQuery) clone.clone(rewrittenType, clone.getOperands());
      }
      return clone;
    }

    /**
     * Rewrite the return type of a windowed aggregate. Calcite's default {@link
     * RexShuttle#visitOver} walks operands and window elements but always constructs the new {@link
     * RexOver} with the ORIGINAL type ({@code rexOver.getType()}). When the aggregate is over a
     * temporal input (e.g. {@code MAX(@timestamp) OVER ()}), the operand type gets rewritten to UDT
     * VARCHAR but the RexOver's cached return type stays as {@code TIMESTAMP(9)} (which lowers to
     * {@code long} in Linq4j codegen). That mismatch causes Calcite's code generator to emit {@code
     * SqlFunctions.greater(long, String)} calls, which fail at runtime. Here we rewrite the return
     * type so it stays consistent with the (rewritten) operand types by rebuilding via {@link
     * RexBuilder#makeOver}.
     */
    @Override
    public RexNode visitOver(RexOver over) {
      RexNode visited = super.visitOver(over);
      if (!(visited instanceof RexOver visitedOver)) {
        return visited;
      }
      RelDataType rewritten = rewriteType(tf, visitedOver.getType());
      if (rewritten == visitedOver.getType()) {
        return visitedOver;
      }
      org.apache.calcite.rex.RexWindow window = visitedOver.getWindow();
      List<RexNode> partitionKeys = window.partitionKeys;
      List<org.apache.calcite.rex.RexFieldCollation> orderKeys = window.orderKeys;
      return new RexBuilder(tf)
          .makeOver(
              rewritten,
              visitedOver.getAggOperator(),
              visitedOver.getOperands(),
              partitionKeys,
              com.google.common.collect.ImmutableList.copyOf(orderKeys),
              window.getLowerBound(),
              window.getUpperBound(),
              window.getExclude(),
              window.isRows(),
              /* allowPartial */ true,
              /* nullWhenCountZero */ false,
              visitedOver.isDistinct(),
              visitedOver.ignoreNulls());
    }

    /**
     * Rewrite temporal types within a lambda body. Calcite's default {@link RexShuttle#visitLambda}
     * walks the body for side effects but discards the rewritten result and returns the original
     * lambda unchanged. Without this override, lambdas (e.g. those used by {@code transform(array,
     * (x, i) -> ...)}) keep standard SQL temporal types in their body even though the array
     * elements at runtime are UDT-backed VARCHARs — which causes {@code ClassCastException: String
     * cannot be cast to Long} when the generated lambda function is applied.
     */
    @Override
    public RexNode visitLambda(RexLambda lambda) {
      RexNode body = lambda.getExpression();
      RexNode newBody = body.accept(this);
      List<RexLambdaRef> params = lambda.getParameters();
      List<RexLambdaRef> newParams = new ArrayList<>(params.size());
      boolean paramsChanged = false;
      for (RexLambdaRef p : params) {
        RelDataType rewritten = rewriteType(tf, p.getType());
        if (rewritten != p.getType()) {
          paramsChanged = true;
          newParams.add(new RexLambdaRef(p.getIndex(), p.getName(), rewritten));
        } else {
          newParams.add(p);
        }
      }
      if (newBody == body && !paramsChanged) {
        return lambda;
      }
      return new RexBuilder(tf).makeLambdaCall(newBody, newParams);
    }

    /**
     * Rewrite the type of a {@link RexLambdaRef} (a parameter reference inside a lambda body).
     * Calcite's default {@link RexShuttle#visitLambdaRef} returns the ref unchanged. Without this
     * override the body of a transformed lambda (see {@link #visitLambda}) would still reference a
     * parameter typed with the original (standard SQL) temporal type, causing the generated UDF
     * code to call {@code longValue()} / {@code Long.valueOf} on a parameter that is actually a
     * {@code String} at runtime.
     */
    @Override
    public RexNode visitLambdaRef(RexLambdaRef ref) {
      RelDataType rewritten = rewriteType(tf, ref.getType());
      if (rewritten == ref.getType()) {
        return ref;
      }
      return new RexLambdaRef(ref.getIndex(), ref.getName(), rewritten);
    }
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.KnnStep.ResolvedK;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.plan.rel.GraphLookup;
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.function.udf.KnnScoreFunction;

/**
 * Builds the canonical k-NN plan, shared by both PPL forms and, later, SQL:
 *
 * <pre>
 * Sort(sort0=[_score], dir0=[DESC-nulls-last], fetch=[k])
 *   Project(&lt;every input column but the scan's _score&gt;, _score=[KNN_SCORE($vec, ARRAY(&lt;REAL
 *       literals&gt;), MAP('k', k)?, MAP('ef_search', n)?, MAP('nprobes', n)?)])
 *     &lt;input: a table scan under Filters and reference-only Projects&gt;
 * </pre>
 *
 * <p>Among the input rows, the plan returns the k with the highest {@code KNN_SCORE}, ordered by
 * it. A pre-filter is a {@code Filter} in the input. An explicit {@code k=} is also kept as {@code
 * MAP('k', k)}, the ANN candidate count, because a later {@code head} may lower the {@code fetch};
 * a k derived from a {@code head} is only the {@code fetch}. The scan's hidden {@code _score}
 * metadata column is left out of the projection, replaced by the k-NN score.
 *
 * <p>All argument validation happens here: <i>knn placement</i>, <i>not a vector field</i>,
 * <i>binary vectors not supported</i>, <i>dimension mismatch</i>, the byte-range check, <i>k out of
 * range</i> and <i>unknown knn option</i> (the latter in {@link KnnOptions}).
 */
public final class KnnPlanBuilder {

  /** Name of the score column, the same as the scan's score metadata column. */
  public static final String SCORE_FIELD = OpenSearchConstants.METADATA_FIELD_SCORE;

  private KnnPlanBuilder() {}

  /**
   * A k-NN step to build.
   *
   * @param field the name of the vector column in the input, as written in the query
   * @param vector the elements of the query vector
   * @param options the validated options; {@code options.k()} is set exactly when k is explicit
   * @param k the resolved k
   */
  public record KnnRequest(
      String field, List<? extends Number> vector, KnnOptions options, ResolvedK k) {
    public KnnRequest {
      Objects.requireNonNull(field, "field");
      Objects.requireNonNull(vector, "vector");
      Objects.requireNonNull(options, "options");
      Objects.requireNonNull(k, "k");
      if (k.explicit() != (options.k() != null)
          || (options.k() != null && options.k() != k.value())) {
        throw new IllegalArgumentException(
            "Resolved k " + k + " does not match the k option " + options.k());
      }
    }
  }

  /**
   * Build the k-NN plan on top of the relation on top of {@code relBuilder}'s stack.
   *
   * @param relBuilder a builder whose top is the k-NN step's input, including any pre-filter
   * @param request the k-NN step
   * @return {@code relBuilder}, whose top is now the {@code Sort} boundary
   * @throws org.opensearch.sql.exception.SemanticCheckException on an invalid placement, field,
   *     vector or k
   */
  public static RelBuilder build(RelBuilder relBuilder, KnnRequest request) {
    RelNode input = relBuilder.peek();
    RelDataTypeField field = checkInput(input, request.field());
    ExprVectorType vectorType = (ExprVectorType) field.getType();
    // Before the other checks: a mismatch also makes the dimension unknown or the data type merged.
    if (vectorType.hasMappingMismatch()) {
      throw KnnErrors.vectorMappingMismatch(
          vectorType.getMappingMismatchIndices(), request.field());
    }
    if (vectorType.getDataType() == VectorDataType.BINARY) {
      throw KnnErrors.binaryVectorsNotSupported();
    }
    if (!vectorType.hasKnownDimension()) {
      throw KnnErrors.unknownDimension(request.field());
    }
    if (!KnnOptions.isValidK(request.k().value())) {
      throw KnnErrors.kOutOfRange(request.k().value());
    }

    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    List<RexNode> scoreArgs = new ArrayList<>();
    scoreArgs.add(relBuilder.field(field.getIndex()));
    scoreArgs.add(queryVector(rexBuilder, vectorType, request));
    KnnOptions options = request.options();
    if (options.k() != null) {
      scoreArgs.add(option(relBuilder, KnnOptions.K, options.k()));
    }
    if (options.efSearch() != null) {
      scoreArgs.add(option(relBuilder, KnnOptions.EF_SEARCH, options.efSearch()));
    }
    if (options.nprobes() != null) {
      scoreArgs.add(option(relBuilder, KnnOptions.NPROBES, options.nprobes()));
    }
    RexNode score = rexBuilder.makeCall(PPLBuiltinOperators.KNN_SCORE, scoreArgs);

    List<RexNode> projects = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (RelDataTypeField inputField : input.getRowType().getFieldList()) {
      if (!SCORE_FIELD.equals(inputField.getName())) {
        projects.add(relBuilder.field(inputField.getIndex()));
        names.add(inputField.getName());
      }
    }
    projects.add(score);
    names.add(SCORE_FIELD);
    relBuilder.project(projects, names);

    RexNode byScore = relBuilder.nullsLast(relBuilder.desc(relBuilder.field(SCORE_FIELD)));
    return relBuilder.sortLimit(0, request.k().value(), List.of(byScore));
  }

  /** Whether {@code node} is a {@code KNN_SCORE} call (matched by name, as backends do). */
  public static boolean isKnnScore(RexNode node) {
    return node instanceof RexCall call
        && KnnScoreFunction.NAME.equals(call.getOperator().getName());
  }

  /**
   * Whether column {@code index} of {@code node} is the k-NN score of a k-NN step below it: a
   * {@code REAL} column, the type of {@code KNN_SCORE}, whose values come from the {@code
   * KNN_SCORE} call or are nulls filled in for rows of an input without one. A column computed from
   * anything else, unnested from an array, or a scan's own {@code _score} is not.
   */
  public static boolean isKnnScoreColumn(RelNode node, int index) {
    if (node.getRowType().getFieldList().get(index).getType().getSqlTypeName()
        != SqlTypeName.REAL) {
      return false;
    }
    Source source = source(node, index, null);
    return source == Source.KNN_SCORE || source == Source.NULLS;
  }

  /**
   * Whether column {@code index} of {@code node} is a column of the same name of a scanned table,
   * passed through unchanged, for some rows at least; other rows may have nulls filled in. A column
   * computed from one, or renamed from another column, is not.
   */
  public static boolean isScanColumn(RelNode node, int index) {
    return source(node, index, node.getRowType().getFieldNames().get(index)) == Source.SCAN;
  }

  /**
   * Whether column {@code index} of {@code node} has no values of its own: it is a scan column of
   * the same name passed through unchanged ({@link #isScanColumn}), or only nulls.
   */
  public static boolean isScanColumnOrNull(RelNode node, int index) {
    Source source = source(node, index, node.getRowType().getFieldNames().get(index));
    return source == Source.SCAN || source == Source.NULLS;
  }

  /** Where the values of a column come from, as far as plan nodes pass it through unchanged. */
  private enum Source {
    /** Only nulls, filled in for rows of an input without the column. */
    NULLS,
    /** The {@code KNN_SCORE} call of a k-NN step. */
    KNN_SCORE,
    /** The scan column of the name traced. */
    SCAN,
    /** Anything else: computed, unnested, mixed, or from a plan node the walk does not know. */
    OTHER;

    /** The source of a column with the values of both, such as the output of a union. */
    Source and(Source other) {
      if (this == other || other == NULLS) {
        return this;
      }
      return this == NULLS ? other : OTHER;
    }
  }

  /**
   * The source of column {@code index} of {@code node}. A column is passed through by projections
   * of references, filters, sorts, joins, correlates, unions, window computations, group keys and
   * the source columns of a graphlookup in its normal mode, possibly cast, or chosen by a {@code
   * CASE}; anything else, including a plan node not listed here, makes it {@link Source#OTHER}.
   *
   * @param scanName the name a scan column must have to be {@link Source#SCAN}; null for none
   */
  private static Source source(RelNode node, int index, @Nullable String scanName) {
    return switch (node) {
      case Project project ->
          source(project.getProjects().get(index), project.getInput(), scanName);
      case Filter filter -> source(filter.getInput(), index, scanName);
      case org.apache.calcite.rel.core.Sort sort -> source(sort.getInput(), index, scanName);
      case Join join -> {
        int leftCount = join.getLeft().getRowType().getFieldCount();
        yield index < leftCount
            ? source(join.getLeft(), index, scanName)
            : source(join.getRight(), index - leftCount, scanName);
      }
      case Correlate correlate -> {
        int leftCount = correlate.getLeft().getRowType().getFieldCount();
        yield index < leftCount
            ? source(correlate.getLeft(), index, scanName)
            : source(correlate.getRight(), index - leftCount, scanName);
      }
      case SetOp setOp ->
          setOp.getInputs().stream()
              .map(input -> source(input, index, scanName))
              .reduce(Source.NULLS, Source::and);
      case Aggregate aggregate ->
          index < aggregate.getGroupCount()
              ? source(aggregate.getInput(), aggregate.getGroupSet().nth(index), scanName)
              : Source.OTHER;
      case Window window ->
          index < window.getInput().getRowType().getFieldCount()
              ? source(window.getInput(), index, scanName)
              : Source.OTHER;
      case Values values ->
          values.getTuples().stream().allMatch(row -> row.get(index).isNull())
              ? Source.NULLS
              : Source.OTHER;
      // Only in its normal mode does a graphlookup pass the source columns through; in batch and
      // literal-start mode, and for the output column, every column is an array.
      case GraphLookup graphLookup ->
          graphLookup.getStartValues() == null
                  && !graphLookup.isBatchMode()
                  && index < graphLookup.getSource().getRowType().getFieldCount()
              ? source(graphLookup.getSource(), index, scanName)
              : Source.OTHER;
      case TableScan scan ->
          scan.getRowType().getFieldNames().get(index).equals(scanName)
              ? Source.SCAN
              : Source.OTHER;
      case Uncollect uncollect -> Source.OTHER;
      default -> Source.OTHER;
    };
  }

  private static Source source(RexNode expression, RelNode input, @Nullable String scanName) {
    return switch (expression) {
      case RexInputRef ref -> source(input, ref.getIndex(), scanName);
      case RexLiteral literal -> literal.isNull() ? Source.NULLS : Source.OTHER;
      case RexCall call when isKnnScore(call) -> Source.KNN_SCORE;
      case RexCall call when call.getKind() == SqlKind.CAST ->
          source(call.getOperands().getFirst(), input, scanName);
      case RexCall call when call.getKind() == SqlKind.CASE -> {
        // CASE(c1, v1, c2, v2, ..., else): the values, not the conditions, make the column.
        List<RexNode> operands = call.getOperands();
        Source source = source(operands.getLast(), input, scanName);
        for (int i = 1; i < operands.size(); i += 2) {
          source = source.and(source(operands.get(i), input, scanName));
        }
        yield source;
      }
      default -> Source.OTHER;
    };
  }

  /**
   * Check the input of a k-NN step: the vector column (<i>not a vector field</i>) and the nodes
   * between it and the scan (<i>knn placement</i>, <i>pre-filter not pushable</i>). {@link #build}
   * does the same first; the lowering calls it on its own to report these before an error about k.
   *
   * @param input the k-NN step's input
   * @param field the name of the vector column, as written in the query
   * @return the vector column
   */
  public static RelDataTypeField checkInput(RelNode input, String field) {
    RelDataTypeField vectorField = vectorField(input, field);
    requirePlacement(input);
    return vectorField;
  }

  /**
   * Whether any expression of any node of {@code plan} contains a {@code KNN_SCORE} call, including
   * the plans of subsearches ({@code in}, {@code exists}, scalar) inside those expressions. Each
   * node is visited once, so a plan that shares nodes stays linear.
   *
   * <p>It follows {@link RelNode#getInputs()}: inside a Volcano or Hep rule the inputs are {@code
   * RelSubset} or {@code HepRelVertex}, so a pushdown rule must not rely on it to see below the
   * node it matched.
   */
  public static boolean containsKnnScore(RelNode plan) {
    return containsKnnScore(plan, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static boolean containsKnnScore(RelNode node, Set<RelNode> visited) {
    if (!visited.add(node)) {
      return false;
    }
    boolean[] found = {false};
    node.accept(
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            if (found[0] || isKnnScore(call)) {
              found[0] = true;
              return call;
            }
            return super.visitCall(call);
          }

          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            // A RexShuttle visits a subsearch's operands but not its plan.
            if (!found[0] && containsKnnScore(subQuery.rel, visited)) {
              found[0] = true;
            }
            return found[0] ? subQuery : super.visitSubQuery(subQuery);
          }
        });
    if (found[0]) {
      return true;
    }
    for (RelNode input : node.getInputs()) {
      if (containsKnnScore(input, visited)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check <i>knn placement</i>: the input must be a table scan under nothing but {@code Filter}s
   * and {@code Project}s of input references, so that the k-NN step and its pre-filter can be
   * pushed into the scan's request as one {@code knn} query. A filter with a subsearch or a
   * correlation cannot be part of that request yet.
   *
   * @param input the k-NN step's input
   * @throws org.opensearch.sql.exception.SemanticCheckException <i>knn placement</i>, or
   *     <i>pre-filter not pushable</i> for a filter with a subsearch
   */
  public static void requirePlacement(RelNode input) {
    RelNode node = input;
    while (!(node instanceof TableScan)) {
      if (node instanceof Filter filter) {
        if (!filter.getVariablesSet().isEmpty()
            || RexUtil.SubQueryFinder.containsSubQuery(filter)) {
          throw KnnErrors.preFilterNotPushable(
              describe(filter.getCondition(), filter.getInput().getRowType()));
        }
        node = filter.getInput();
      } else if (isReferenceOnly(node)) {
        node = node.getInput(0);
      } else {
        throw KnnErrors.knnPlacement();
      }
    }
  }

  /**
   * A readable form of a filter condition for an error message: column names instead of input
   * references, and {@code [subsearch]} instead of a subsearch's plan.
   */
  private static String describe(RexNode node, RelDataType inputRowType) {
    if (node instanceof RexInputRef ref) {
      return inputRowType.getFieldNames().get(ref.getIndex());
    }
    if (node instanceof RexSubQuery subQuery) {
      List<String> operands = new ArrayList<>();
      subQuery.getOperands().forEach(operand -> operands.add(describe(operand, inputRowType)));
      operands.add("[subsearch]");
      return subQuery.getOperator().getName() + "(" + String.join(", ", operands) + ")";
    }
    if (node instanceof RexCall call) {
      List<String> operands = new ArrayList<>();
      call.getOperands().forEach(operand -> operands.add(describe(operand, inputRowType)));
      return call.getOperator().getName() + "(" + String.join(", ", operands) + ")";
    }
    return node.toString();
  }

  private static boolean isReferenceOnly(RelNode node) {
    return node instanceof Project project
        && project.getProjects().stream().allMatch(RexInputRef.class::isInstance);
  }

  /**
   * The vector column the k-NN step searches.
   *
   * @throws org.opensearch.sql.exception.SemanticCheckException <i>not a vector field</i> if the
   *     column is missing, not a vector, or computed by a projection
   */
  private static RelDataTypeField vectorField(RelNode input, String name) {
    RelDataTypeField field = input.getRowType().getField(name, true, false);
    if (field == null
        || !(field.getType() instanceof ExprVectorType)
        || isComputed(input, field.getIndex())) {
      throw KnnErrors.notAVectorField(name);
    }
    return field;
  }

  /**
   * Whether a projection between {@code node} and the scan below computes column {@code index}
   * rather than passing a scan column through. Other nodes in between are left to {@link
   * #requirePlacement}.
   */
  private static boolean isComputed(RelNode node, int index) {
    while (node instanceof Filter || node instanceof Project) {
      if (node instanceof Project project) {
        if (!(project.getProjects().get(index) instanceof RexInputRef ref)) {
          return true;
        }
        index = ref.getIndex();
      }
      node = node.getInput(0);
    }
    return false;
  }

  /**
   * The query vector as {@code ARRAY(<REAL literals>)}, after checking its length against the
   * field's dimension and, for {@code byte} fields, that every element is an integer in [-128,
   * 127].
   */
  private static RexNode queryVector(
      RexBuilder rexBuilder, ExprVectorType vectorType, KnnRequest request) {
    List<? extends Number> vector = request.vector();
    if (vector.isEmpty()) {
      throw KnnErrors.invalidVector(vector);
    }
    if (vector.size() != vectorType.getDimension()) {
      throw KnnErrors.dimensionMismatch(vector.size(), request.field(), vectorType.getDimension());
    }
    RelDataType real = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.REAL);
    List<RexNode> elements = new ArrayList<>(vector.size());
    for (int i = 0; i < vector.size(); i++) {
      Number element = vector.get(i);
      BigDecimal value = toBigDecimal(element);
      if (value == null) {
        throw KnnErrors.invalidVector(vector);
      }
      if (vectorType.getDataType() == VectorDataType.BYTE && !isByte(value)) {
        throw KnnErrors.byteElementOutOfRange(request.field(), i, element);
      }
      elements.add(rexBuilder.makeApproxLiteral(value, real));
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, elements);
  }

  /** The exact decimal value of a finite number, or {@code null} for null, NaN or infinity. */
  private static @Nullable BigDecimal toBigDecimal(@Nullable Number number) {
    if (number == null) {
      return null;
    }
    if (number instanceof BigDecimal decimal) {
      return decimal;
    }
    try {
      return new BigDecimal(number.toString());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean isByte(BigDecimal value) {
    return value.stripTrailingZeros().scale() <= 0
        && value.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) >= 0
        && value.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) <= 0;
  }

  /** One option, encoded as {@code MAP_VALUE_CONSTRUCTOR('name', value)}. */
  private static RexNode option(RelBuilder relBuilder, String name, int value) {
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    return rexBuilder.makeCall(
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
        rexBuilder.makeLiteral(name),
        relBuilder.literal(value));
  }
}

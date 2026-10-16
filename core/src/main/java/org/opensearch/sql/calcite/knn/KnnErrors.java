/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * User errors of PPL k-NN search. Each factory method is named after an error case of the PPL k-NN
 * design and returns a {@link SemanticCheckException}: the REST layer reports it as HTTP 400, and,
 * since it is marked {@link SemanticCheckException#withoutCalciteFallback()}, it never makes the
 * query fall back to the V2 engine, even with {@code plugins.calcite.fallback.allowed}: V2 cannot
 * run a k-NN step, so the attempt could only fail (and would then report this error anyway).
 */
public final class KnnErrors {

  /** Message of <i>missing k</i> when nothing stopped the derivation of k. */
  public static final String MISSING_K_MESSAGE = "knn requires k or a following head";

  /** Message of <i>knn under OR or NOT</i>. */
  public static final String KNN_UNDER_OR_OR_NOT_MESSAGE =
      "knn() must be a top-level conjunct of where";

  /** Message of <i>more than one knn</i>. */
  public static final String MORE_THAN_ONE_KNN_MESSAGE = "only one knn per query branch";

  /** Message of <i>knn placement</i>. */
  public static final String KNN_PLACEMENT_MESSAGE =
      "knn must directly follow source, where or fields";

  /** Message of <i>Calcite engine required</i>. */
  public static final String CALCITE_ENGINE_REQUIRED_MESSAGE = "knn requires the Calcite engine";

  /** Message of <i>analytics engine not supported</i>. */
  public static final String ANALYTICS_ENGINE_NOT_SUPPORTED_MESSAGE =
      "knn is not supported on the analytics engine yet";

  /** Message of <i>binary vectors not supported</i>. */
  public static final String BINARY_VECTORS_NOT_SUPPORTED_MESSAGE =
      "knn on binary vectors is not supported yet";

  /** Message of the error raised when a plan with {@code KNN_SCORE} is transpiled to SQL. */
  public static final String NOT_TRANSPILABLE_MESSAGE =
      "knn cannot be transpiled to SQL: KNN_SCORE has no equivalent in the target dialect";

  /** Message of the error raised when a {@code KNN_SCORE} call is executed instead of pushed. */
  public static final String NOT_PUSHED_DOWN_MESSAGE = "knn is only supported when pushed down";

  private KnnErrors() {}

  /**
   * A k-NN error, marked so that the query never falls back to the V2 engine, even with {@code
   * plugins.calcite.fallback.allowed}.
   */
  private static SemanticCheckException error(String message) {
    return new SemanticCheckException(message).withoutCalciteFallback();
  }

  /**
   * <i>missing k</i>: no {@code k=} and no {@code head} that k can be derived from.
   *
   * @param stoppedAt the command that stopped the derivation, e.g. {@code sort}, or {@code null} if
   *     there is no {@code head} after the k-NN step at all
   */
  public static SemanticCheckException missingK(@Nullable String stoppedAt) {
    if (stoppedAt == null) {
      return error(MISSING_K_MESSAGE);
    }
    return error(
        String.format(
            "%s; k cannot be derived from a head after [%s], set k explicitly, e.g. k=10",
            MISSING_K_MESSAGE, stoppedAt));
  }

  /**
   * <i>k out of range</i>: an explicit {@code k=} that is not an integer in [1, 10000].
   *
   * @param value the value given for k
   */
  public static SemanticCheckException kOutOfRange(Object value) {
    return error(String.format("k must be between 1 and %d, got [%s]", KnnOptions.MAX_K, value));
  }

  /**
   * <i>k out of range</i> for a k derived from a {@code head}.
   *
   * @param headSize the size of the {@code head} k would be derived from
   */
  public static SemanticCheckException derivedKOutOfRange(int headSize) {
    return error(
        String.format(
            "k must be between 1 and %d, but would be derived from [head %d]; set k explicitly",
            KnnOptions.MAX_K, headSize));
  }

  /**
   * An explicit k followed by {@code head N from M} with {@code M + N > k}: the head asks for rows
   * the k-NN step never returns.
   */
  public static SemanticCheckException headBeyondK(int size, int offset, int k) {
    return error(
        String.format(
            "head %d from %d needs %d rows, but knn returns at most k=%d; increase k",
            size, offset, size + offset, k));
  }

  /**
   * <i>not a vector field</i>: the field is missing, computed, or not a {@code knn_vector} field.
   *
   * @param field the field as written in the query
   */
  public static SemanticCheckException notAVectorField(String field) {
    return error(String.format("knn field [%s] is not a knn_vector field", field));
  }

  /**
   * <i>not a vector field</i> for a {@code knn_vector} field whose mapping declares no dimension,
   * such as a model-based field.
   */
  public static SemanticCheckException unknownDimension(String field) {
    return error(
        String.format(
            "knn field [%s] is not a knn_vector field with a dimension in its mapping (e.g. it is"
                + " model-based); knn needs the dimension",
            field));
  }

  /**
   * <i>vector mapping mismatch</i>: the indices of the query map the k-NN field with a different
   * dimension, data type or space type.
   *
   * @param indices the indices, sorted, or empty if they are not known
   * @param field the k-NN field
   */
  public static SemanticCheckException vectorMappingMismatch(List<String> indices, String field) {
    return error(
        indices.isEmpty()
            ? String.format(
                "the indices of the query have different vector mappings for [%s]", field)
            : String.format(
                "indices [%s] have different vector mappings for [%s]",
                String.join(", ", indices), field));
  }

  /** <i>binary vectors not supported</i>. */
  public static SemanticCheckException binaryVectorsNotSupported() {
    return error(BINARY_VECTORS_NOT_SUPPORTED_MESSAGE);
  }

  /** <i>dimension mismatch</i>: the query vector's length differs from the field's dimension. */
  public static SemanticCheckException dimensionMismatch(
      int vectorLength, String field, int dimension) {
    return error(
        String.format(
            "vector has [%d] dimensions; field [%s] has [%d]", vectorLength, field, dimension));
  }

  /** The query vector is not a non-empty {@code [...]} / {@code array(...)} of numeric literals. */
  public static SemanticCheckException invalidVector(Object vector) {
    return error(
        String.format(
            "knn vector must be a non-empty array of numeric literals, got [%s]", vector));
  }

  /** An element of a query vector for a {@code byte} field is not an integer in [-128, 127]. */
  public static SemanticCheckException byteElementOutOfRange(
      String field, int position, Object value) {
    return error(
        String.format(
            "knn field [%s] holds byte vectors; vector element %d [%s] must be an integer in"
                + " [-128, 127]",
            field, position, value));
  }

  /** <i>unknown knn option</i>. */
  public static SemanticCheckException unknownOption(String option) {
    return error(String.format("unknown knn option [%s]", option));
  }

  /** A knn option given twice. */
  public static SemanticCheckException duplicateOption(String option) {
    return error(String.format("duplicate knn option [%s]", option));
  }

  /** A knn option other than k whose value is not a positive integer. */
  public static SemanticCheckException invalidOptionValue(String option, Object value) {
    return error(
        String.format("knn option [%s] must be a positive integer, got [%s]", option, value));
  }

  /** A {@code knn(...)} call without both a field and a query vector. */
  public static SemanticCheckException wrongArguments(Object call) {
    return error(
        String.format(
            "knn requires a knn_vector field and a query vector, e.g. knn(<field>, [<v1>, <v2>,"
                + " ...], k=<n>), got [%s]",
            call));
  }

  /** A {@code knn(...)} argument after the vector that is not {@code name=value}. */
  public static SemanticCheckException unnamedOption(Object argument) {
    return error(String.format("knn options must be given as name=value, got [%s]", argument));
  }

  /**
   * <i>pre-filter not pushable</i>: a filter below the k-NN step cannot become the {@code filter}
   * of the OpenSearch {@code knn} query, e.g. because it has a subsearch.
   *
   * @param preFilter the filter, as text
   */
  public static SemanticCheckException preFilterNotPushable(String preFilter) {
    return error(
        String.format(
            "pre-filter [%s] cannot be pushed into the k-NN filter; move it after knn as a"
                + " post-filter",
            preFilter));
  }

  /**
   * A plan with {@code KNN_SCORE} cannot be transpiled to another SQL dialect: none has the k-NN
   * plugin's approximate nearest-neighbour search.
   */
  public static SemanticCheckException notTranspilable() {
    return error(NOT_TRANSPILABLE_MESSAGE);
  }

  /** <i>knn under OR or NOT</i>. */
  public static SemanticCheckException knnUnderOrOrNot() {
    return error(KNN_UNDER_OR_OR_NOT_MESSAGE);
  }

  /** <i>more than one knn</i>. */
  public static SemanticCheckException moreThanOneKnn() {
    return error(MORE_THAN_ONE_KNN_MESSAGE);
  }

  /** <i>knn placement</i>. */
  public static SemanticCheckException knnPlacement() {
    return error(KNN_PLACEMENT_MESSAGE);
  }

  /**
   * A join without keys whose inputs both have a k-NN {@code _score}: as a column both inputs
   * share, {@code _score} would be one of the implicit keys, joining rows on equal scores.
   */
  public static SemanticCheckException keylessJoinOnScore() {
    return error(
        "a join without keys would join on [_score], which both inputs have from their k-NN steps."
            + " Remove it from one input with `fields - _score`, or name the join keys");
  }

  /**
   * A command after a k-NN step would replace {@code _score}: in a k-NN branch {@code _score} is
   * the k-NN score, and only commands that pass it through may output a column of that name.
   *
   * @param command the command, e.g. {@code rename} or {@code lookup}
   */
  public static SemanticCheckException scoreReplaced(String command) {
    return error(
        String.format(
            "after knn, _score is the k-NN score; [%s] cannot replace it, give the result another"
                + " name",
            command));
  }

  /**
   * A lookup after a k-NN step names the lookup table's {@code _score}, as a key or an output: the
   * table's hidden {@code _score} is dropped, as in a k-NN branch {@code _score} is the k-NN score.
   */
  public static SemanticCheckException lookupTableScore() {
    return error(
        "after knn, _score is the k-NN score and cannot be taken from the lookup table; remove"
            + " _score from the lookup fields");
  }

  /**
   * An input combined with a k-NN one has only the scan's hidden {@code _score} column. That column
   * is dropped, as the combined {@code _score} is the k-NN score, which would leave the input with
   * no column at all.
   *
   * @param command the combining command, e.g. {@code join}
   */
  public static SemanticCheckException onlyMetadataScore(String command) {
    return error(
        String.format(
            "an input of [%s] has only the scan's _score metadata column, which is dropped when"
                + " the input is combined with a k-NN input; select other fields of that input",
            command));
  }

  /**
   * A command would add a second {@code _score} column to the rows of a k-NN step or to rows
   * combined with them: one named {@code _score} next to an existing one, or one with the name
   * Calcite gives such a column, such as {@code _score0}. Whether Calcite renamed it or the query
   * named it so itself, the result would show a column that looks like another score, next to the
   * k-NN score or in its place.
   *
   * @param command the command, e.g. {@code expand}
   * @param column the column name, e.g. {@code _score0}
   */
  public static SemanticCheckException scoreRenamed(String command, String column) {
    return error(
        String.format(
            "k-NN results, and rows combined with them, cannot have a second _score column ([%s]);"
                + " give the column that [%s] adds another name",
            column, command));
  }

  /**
   * An input without a k-NN step, combined with a k-NN input, has a {@code _score} the query made
   * itself rather than the scan's. Combined, {@code _score} is the k-NN score, so that input's
   * {@code _score} would be nulled or dropped without a word.
   *
   * @param command the combining command, e.g. {@code join}
   */
  public static SemanticCheckException inputScore(String command) {
    return error(
        String.format(
            "an input of [%s] without knn has a _score column of its own, which would be lost:"
                + " combined with a k-NN input, _score is the k-NN score; give the column another"
                + " name",
            command));
  }

  /** <i>Calcite engine required</i>: the V2 engine was asked to run a k-NN step. */
  public static SemanticCheckException calciteEngineRequired() {
    return error(CALCITE_ENGINE_REQUIRED_MESSAGE);
  }

  /** <i>analytics engine not supported</i>. */
  public static SemanticCheckException analyticsEngineNotSupported() {
    return error(ANALYTICS_ENGINE_NOT_SUPPORTED_MESSAGE);
  }

  /** A {@code KNN_SCORE} call reached execution because no rule pushed it into the scan. */
  public static SemanticCheckException notPushedDown() {
    return error(NOT_PUSHED_DOWN_MESSAGE);
  }
}

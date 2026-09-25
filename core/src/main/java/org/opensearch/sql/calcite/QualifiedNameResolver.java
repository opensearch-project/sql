/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Utility class for resolving qualified names in Calcite query planning. Extracts the qualified
 * name resolution logic from CalciteRexNodeVisitor to provide a centralized and reusable
 * implementation.
 */
public class QualifiedNameResolver {

  private static final Logger log = LogManager.getLogger(QualifiedNameResolver.class);

  /**
   * Resolves a qualified name to a RexNode based on the current context.
   *
   * @param nameNode The QualifiedName to resolve
   * @param context The CalcitePlanContext containing the current state
   * @return RexNode representing the resolved qualified name
   * @throws IllegalArgumentException if the field is not found in the current context
   */
  public static RexNode resolve(QualifiedName nameNode, CalcitePlanContext context) {
    log.debug(
        "QualifiedNameResolver.resolve() called with nameNode={}, isResolvingJoinCondition={}",
        nameNode,
        context.isResolvingJoinCondition());

    if (context.isResolvingJoinCondition()) {
      return resolveInJoinCondition(nameNode, context);
    } else {
      return resolveInNonJoinCondition(nameNode, context);
    }
  }

  /** Resolves qualified name in join condition context. */
  private static RexNode resolveInJoinCondition(
      QualifiedName nameNode, CalcitePlanContext context) {
    log.debug("resolveInJoinCondition() called with nameNode={}", nameNode);

    return resolveFieldWithAlias(nameNode, context, 2)
        .or(() -> resolveFieldWithoutAlias(nameNode, context, 2))
        .orElseThrow(() -> getNotFoundException(nameNode, context));
  }

  /** Resolves qualified name in non-join condition context. */
  private static RexNode resolveInNonJoinCondition(
      QualifiedName nameNode, CalcitePlanContext context) {
    log.debug("resolveInNonJoinCondition() called with nameNode={}", nameNode);

    // First try to resolve as lambda variable
    Optional<RexNode> lambdaVar = resolveLambdaVariable(nameNode, context);
    if (lambdaVar.isPresent()) {
      return lambdaVar.get();
    }

    // Try to resolve as regular field
    Optional<RexNode> fieldRef =
        resolveFieldDirectly(nameNode, context, 1)
            .or(() -> resolveFieldWithAlias(nameNode, context, 1))
            .or(() -> resolveFieldWithoutAlias(nameNode, context, 1))
            .or(() -> resolveRenamedField(nameNode, context));

    if (fieldRef.isPresent()) {
      // If we're in a lambda context and this is not a lambda variable,
      // we need to capture it as an external variable
      if (context.isInLambdaContext()) {
        log.debug("Capturing external field {} in lambda context", nameNode);
        return context.captureVariable(fieldRef.get(), nameNode.toString());
      }
      return fieldRef.get();
    }

    return resolveCorrelationField(nameNode, context)
        .or(() -> replaceWithNullLiteralInCoalesce(context))
        .orElseThrow(() -> getNotFoundException(nameNode, context));
  }

  private static String joinParts(List<String> parts, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        sb.append(".");
      }
      sb.append(parts.get(start + i));
    }
    return sb.toString();
  }

  private static String joinParts(List<String> parts, int start) {
    return joinParts(parts, start, parts.size() - start);
  }

  private static Optional<RexNode> resolveFieldDirectly(
      QualifiedName nameNode, CalcitePlanContext context, int inputCount) {
    List<String> parts = nameNode.getParts();
    log.debug(
        "resolveFieldDirectly() called with nameNode={}, parts={}, inputCount={}",
        nameNode,
        parts,
        inputCount);

    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    if (currentFields.contains(nameNode.toString())) {
      try {
        return Optional.of(context.relBuilder.field(nameNode.toString()));
      } catch (IllegalArgumentException e) {
        log.debug("resolveFieldDirectly() failed: {}", e.getMessage());
      }
    }
    return Optional.empty();
  }

  private static Optional<RexNode> resolveFieldWithAlias(
      QualifiedName nameNode, CalcitePlanContext context, int inputCount) {
    List<String> parts = nameNode.getParts();
    log.debug(
        "resolveFieldWithAlias() called with nameNode={}, parts={}, inputCount={}",
        nameNode,
        parts,
        inputCount);

    if (parts.size() >= 2) {
      // Consider first part as table alias
      String alias = parts.get(0);
      log.debug("resolveFieldWithAlias() trying alias={}", alias);

      // Try to resolve the longest match first
      for (int length = parts.size() - 1; 1 <= length; length--) {
        String field = joinParts(parts, 1, length);
        log.debug("resolveFieldWithAlias() trying field={} with length={}", field, length);

        Optional<RexNode> fieldNode = tryToResolveField(alias, field, context, inputCount);
        if (fieldNode.isPresent()) {
          return Optional.of(
              resolveFieldAccess(context, parts, 1, length, fieldNode.get(), false));
        }
      }
    }
    return Optional.empty();
  }

  private static Optional<RexNode> tryToResolveField(
      String alias, String fieldName, CalcitePlanContext context, int inputCount) {
    log.debug(
        "tryToResolveField() called with alias={}, fieldName={}, inputCount={}",
        alias,
        fieldName,
        inputCount);
    try {
      return Optional.of(context.relBuilder.field(inputCount, alias, fieldName));
    } catch (IllegalArgumentException e) {
      log.debug("tryToResolveField() failed: {}", e.getMessage());
    }
    return Optional.empty();
  }

  private static Optional<RexNode> resolveFieldWithoutAlias(
      QualifiedName nameNode, CalcitePlanContext context, int inputCount) {
    log.debug(
        "resolveFieldWithoutAlias() called with nameNode={}, inputCount={}", nameNode, inputCount);

    List<Set<String>> inputFieldNames = collectInputFieldNames(context, inputCount);

    List<String> parts = nameNode.getParts();
    Optional<RexNode> resolved =
        resolveFromParts(parts, context, inputCount, inputFieldNames, false);
    if (resolved.isPresent()) {
      return resolved;
    }
    // A quoted identifier that contains dots (`cluster.name`) is a single part, so the walk above
    // has
    // nothing to descend and only matches a column literally called that. Vanilla flattens objects,
    // so
    // there such a column really exists; where an object is a struct instead, the same name means
    // the
    // path into it. Split and retry, but only after the literal lookup failed, so a column named
    // with dots still wins.
    //
    // The retry only accepts a resolution that descends a ROW (structDescentOnly). A quoted dotted
    // name must not turn into a map key: a column whose dotted subtree was shed -- as happens when
    // a container column is rebuilt, e.g. a second `spath output=data` dropping a `data.custom`
    // created in between -- is meant to be unreachable, and splitting it into ITEM(data, 'custom')
    // would silently resolve it again and return null instead of reporting the field as gone.
    List<String> split = new ArrayList<>(parts.size());
    for (String part : parts) {
      split.addAll(List.of(part.split("\\.")));
    }
    if (split.size() != parts.size()) {
      return resolveFromParts(split, context, inputCount, inputFieldNames, true);
    }
    return Optional.empty();
  }

  /**
   * Longest-prefix match over {@code parts}, descending whatever is left into the matched field.
   *
   * @param structDescentOnly accept a match only when the leftover path is consumed by descending a
   *     ROW, never by becoming an ITEM key. Set when retrying a quoted dotted name that was split,
   *     where turning the name into a map key would resolve a field that is meant to be gone.
   */
  private static Optional<RexNode> resolveFromParts(
      List<String> parts,
      CalcitePlanContext context,
      int inputCount,
      List<Set<String>> inputFieldNames,
      boolean structDescentOnly) {
    for (int length = parts.size(); 1 <= length; length--) {
      String fieldName = joinParts(parts, 0, length);
      log.debug("resolveFromParts() trying fieldName={} with length={}", fieldName, length);

      int foundInput = findInputContainingFieldName(inputCount, inputFieldNames, fieldName);
      if (foundInput != -1) {
        RexNode fieldNode = context.relBuilder.field(inputCount, foundInput, fieldName);
        RexNode resolved =
            resolveFieldAccess(context, parts, 0, length, fieldNode, structDescentOnly);
        if (resolved != null) {
          return Optional.of(resolved);
        }
        // This prefix matched a column but the rest of the path is not in its ROW. A shorter prefix
        // may still match a different column, so keep walking rather than giving up here.
      }
    }
    return Optional.empty();
  }

  private static int findInputContainingFieldName(
      int inputCount, List<Set<String>> inputFieldNames, String fieldName) {
    int foundInput = -1;
    for (int i = 0; i < inputCount; i++) {
      if (inputFieldNames.get(i).contains(fieldName)) {
        if (foundInput != -1) {
          throw new IllegalArgumentException("Ambiguous field: " + fieldName);
        } else {
          foundInput = i;
        }
      }
    }
    return foundInput;
  }

  private static List<Set<String>> collectInputFieldNames(
      CalcitePlanContext context, int inputCount) {
    List<Set<String>> inputFieldNames = new ArrayList<>();
    for (int i = 0; i < inputCount; i++) {
      int inputOrdinal = inputCount - i - 1;
      Set<String> fieldNames =
          context.relBuilder.peek(inputOrdinal).getRowType().getFieldList().stream()
              .map(RelDataTypeField::getName)
              .collect(Collectors.toSet());
      inputFieldNames.add(fieldNames);
      log.debug("collectInputFieldNames() input[{}] fieldNames={}", inputOrdinal, fieldNames);
    }
    return inputFieldNames;
  }

  /** Try to resolve renamed field due to duplicate field name while join. e.g. alias.fieldName */
  private static Optional<RexNode> resolveRenamedField(
      QualifiedName nameNode, CalcitePlanContext context) {
    log.debug("resolveRenamedField() called with nameNode={}", nameNode);

    List<String> parts = nameNode.getParts();
    if (parts.size() >= 2) {
      List<String> candidates = findCandidatesByRenamedFieldName(nameNode, context);
      String alias = parts.get(0);
      for (String candidate : candidates) {
        try {
          return Optional.of(context.relBuilder.field(alias, candidate));
        } catch (IllegalArgumentException e1) {
          // Indicates the field was not found.
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Find the original name before fieldName is renamed due to duplicate field name. Example:
   * renamedFieldname = "alias.fieldName", originalFieldName = "fieldName"
   */
  private static List<String> findCandidatesByRenamedFieldName(
      QualifiedName renamedFieldName, CalcitePlanContext context) {
    String originalFieldName = joinParts(renamedFieldName.getParts(), 1);
    return context.relBuilder.peek().getRowType().getFieldNames().stream()
        .filter(col -> getNameBeforeRename(col).equals(originalFieldName))
        .toList();
  }

  private static String getNameBeforeRename(String fieldName) {
    return fieldName.substring(fieldName.indexOf(".") + 1);
  }

  private static Optional<RexNode> resolveCorrelationField(
      QualifiedName nameNode, CalcitePlanContext context) {
    log.debug("resolveCorrelationField() called with nameNode={}", nameNode);
    List<String> parts = nameNode.getParts();
    return context
        .peekCorrelVar()
        .map(
            correlation -> {
              List<String> fieldNameList = correlation.getType().getFieldNames();
              // Try full match, then consider first part as table alias
              for (int start = 0; start <= 1; start++) {
                // Try to resolve the longest match first
                for (int length = parts.size() - start; 1 <= length; length--) {
                  String fieldName = joinParts(parts, start, length);
                  log.debug("resolveCorrelationField() trying fieldName={}", fieldName);
                  if (fieldNameList.contains(fieldName)) {
                    RexNode field = context.relBuilder.field(correlation, fieldName);
                    return resolveFieldAccess(context, parts, start, length, field, false);
                  }
                }
              }
              return null;
            });
  }

  /**
   * Resolves the path segments left over after {@code field} was matched.
   *
   * <p>A ROW is descended one segment at a time with Calcite's native field access, which is how a
   * struct is referenced: {@code $1.location.latitude} is two accesses, each typed by the child it
   * reads. Anything else keeps the whole remainder as a single {@code ITEM} key, which is right for
   * a MAP: a vanilla {@code object} is typed {@code MAP<VARCHAR, ANY>} because vanilla stores
   * objects flattened, so {@code city.location.latitude} genuinely is one key there.
   *
   * <p>Joining the remainder unconditionally, as this did before, produced {@code ITEM($city,
   * 'location.latitude')} for a real nested struct. {@code SqlItemOperator} looks a dotted key up
   * as one field name, finds nothing, and throws {@code AssertionError: Cannot infer type of field
   * ... within ROW type}. Being an Error it escapes the {@code catch (Exception)} in the resolve
   * loop and surfaces as a 500. That made every struct path deeper than one segment unreachable.
   *
   * @return the resolved node, or {@code null} when descent stopped inside a ROW on a segment that
   *     names no field of it, which is an unresolved path rather than a usable node
   */
  private static RexNode resolveFieldAccess(
      CalcitePlanContext context,
      List<String> parts,
      int start,
      int length,
      RexNode field,
      boolean structDescentOnly) {
    int remaining = length + start;
    RexNode current = field;
    while (remaining < parts.size() && current.getType().isStruct()) {
      RelDataTypeField child = current.getType().getField(parts.get(remaining), false, false);
      if (child == null) {
        break;
      }
      current = context.rexBuilder.makeFieldAccess(current, child.getIndex());
      remaining++;
    }
    if (remaining == parts.size()) {
      return current;
    }
    if (current.getType().isStruct()) {
      // Descent stopped on a segment that names no field of this ROW. Joining the remainder into an
      // ITEM key here would rebuild the very shape this method exists to avoid: ITEM(<ROW>, 'x')
      // makes SqlItemOperator throw the AssertionError described above, which escapes the caller's
      // catch (Exception) as a 500. Report it as unresolved so it reaches the ordinary
      // "Field [...] not found" instead.
      return null;
    }
    if (structDescentOnly) {
      // Reached only from the split retry of a quoted dotted name. The leftover would become a map
      // key, which is not what the quoted name asked for, so report it unresolved.
      return null;
    }
    return createItemAccess(
        current, joinParts(parts, remaining, parts.size() - remaining), context);
  }

  private static RexNode createItemAccess(
      RexNode field, String itemName, CalcitePlanContext context) {
    log.debug("createItemAccess() called with itemName={}", itemName);
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder,
        BuiltinFunctionName.INTERNAL_ITEM,
        field,
        context.rexBuilder.makeLiteral(itemName));
  }

  private static Optional<RexNode> resolveLambdaVariable(
      QualifiedName nameNode, CalcitePlanContext context) {
    log.debug("resolveLambdaVariable() called with nameNode={}", nameNode);
    String qualifiedName = nameNode.toString();
    return Optional.ofNullable(context.getRexLambdaRefMap().get(qualifiedName));
  }

  private static Optional<RexNode> replaceWithNullLiteralInCoalesce(CalcitePlanContext context) {
    log.debug("replaceWithNullLiteralInCoalesce() called");
    if (context.isInCoalesceFunction()) {
      // Use SqlTypeName.NULL so the resulting literal does not bias the least-restrictive
      // common-type computation toward VARCHAR. See issue #5175: previously VARCHAR was used,
      // which caused COALESCE(null, 42) to be inferred as VARCHAR and returned as "42".
      return Optional.of(
          context.rexBuilder.makeNullLiteral(
              context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.NULL)));
    }
    return Optional.empty();
  }

  private static ErrorReport getNotFoundException(QualifiedName node, CalcitePlanContext context) {
    // Collect all available fields from the current context
    List<String> availableFields = context.relBuilder.peek().getRowType().getFieldNames();

    ErrorReport.Builder builder =
        ErrorReport.wrap(
                new IllegalArgumentException(
                    String.format("Field [%s] not found.", node.toString())))
            .code(ErrorCode.FIELD_NOT_FOUND)
            .context("requested_field", node.toString())
            .context("available_fields", availableFields);

    // Add a suggestion based on Levenshtein distance
    StringUtils.findClosestMatch(node.toString(), availableFields)
        .ifPresent(suggestion -> builder.suggestion("Did you mean: " + suggestion));

    // Add source position if available (populated by PPL parser)
    if (node.getLine() != null && node.getColumn() != null) {
      builder.context("query_pos", Map.of("line", node.getLine(), "column", node.getColumn()));
    }

    return builder.build();
  }
}

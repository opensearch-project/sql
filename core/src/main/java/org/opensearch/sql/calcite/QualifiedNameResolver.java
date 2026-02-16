/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.expression.QualifiedName;
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
        .orElseThrow(() -> getNotFoundException(nameNode));
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
        .orElseThrow(() -> getNotFoundException(nameNode));
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
          return Optional.of(resolveFieldAccess(context, parts, 1, length, fieldNode.get()));
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
    for (int length = parts.size(); 1 <= length; length--) {
      String fieldName = joinParts(parts, 0, length);
      log.debug("resolveFieldWithoutAlias() trying fieldName={} with length={}", fieldName, length);

      int foundInput = findInputContainingFieldName(inputCount, inputFieldNames, fieldName);
      log.debug("resolveFieldWithoutAlias() foundInput={}", foundInput);
      if (foundInput != -1) {
        RexNode fieldNode = context.relBuilder.field(inputCount, foundInput, fieldName);
        return Optional.of(resolveFieldAccess(context, parts, 0, length, fieldNode));
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
                    return resolveFieldAccess(context, parts, start, length, field);
                  }
                }
              }
              return null;
            });
  }

  private static RexNode resolveFieldAccess(
      CalcitePlanContext context, List<String> parts, int start, int length, RexNode field) {
    if (length == parts.size() - start) {
      return field;
    } else {
      String itemName = joinParts(parts, length + start, parts.size() - length);
      return context.relBuilder.alias(
          createItemAccess(field, itemName, context),
          String.join(QualifiedName.DELIMITER, parts.subList(start, parts.size())));
    }
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
      return Optional.of(
          context.rexBuilder.makeNullLiteral(
              context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR)));
    }
    return Optional.empty();
  }

  private static RuntimeException getNotFoundException(QualifiedName node) {
    return new IllegalArgumentException(String.format("Field [%s] not found.", node.toString()));
  }
}

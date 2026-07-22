/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.gson;

import com.google.gson.JsonSyntaxException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.ForeachPlaceholder;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Foreach;
import org.opensearch.sql.ast.tree.Foreach.ForeachEvalClause;
import org.opensearch.sql.calcite.CalcitePlanContext.ForeachBinding;
import org.opensearch.sql.calcite.CalcitePlanContext.ForeachBindingType;
import org.opensearch.sql.calcite.utils.WildcardUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Plans the PPL {@code foreach} command.
 *
 * <p>Multifield mode expands each eval clause once per matching field, binding {@code <<FIELD>>}
 * (and match placeholders) so the clause expression resolves against the current field.
 *
 * <p>Collection modes (multivalue / json_array / auto_collections) rewrite each eval clause into a
 * {@code reduce} over the collection: elements are packed into internal pairs {@code [item, iter,
 * captured-field...]} so the lambda can reference the loop item, the loop index, and any row fields
 * the clause mentions. Placeholder bindings are staged on the context and only become active inside
 * the lambda; the reduce call's other arguments resolve against the row as usual.
 */
class ForeachPlanner {

  private static final String OPTION_FIELDSTR = "fieldstr";
  private static final String OPTION_MATCHSTR = "matchstr";
  private static final String OPTION_MATCHSEG = "matchseg";
  private static final String OPTION_ITEMSTR = "itemstr";
  private static final String OPTION_ITERSTR = "iterstr";
  private static final String PLACEHOLDER_FIELD = "FIELD";
  private static final String PLACEHOLDER_MATCHSTR = "MATCHSTR";
  private static final String PLACEHOLDER_MATCHSEG = "MATCHSEG";
  private static final String PLACEHOLDER_ITEM = "ITEM";
  private static final String PLACEHOLDER_ITER = "ITER";

  /** Name of the lambda variable holding the internal pair inside generated reduce calls. */
  private static final String PAIR_VAR = "__foreach_pair";

  private static final String STATE_VAR = "__foreach_state";

  private static final Set<String> ARITHMETIC_OPERATORS = Set.of("+", "-", "*", "/", "%");

  private final CalciteRelNodeVisitor relVisitor;
  private final CalciteRexNodeVisitor rexVisitor;

  ForeachPlanner(CalciteRelNodeVisitor relVisitor, CalciteRexNodeVisitor rexVisitor) {
    this.relVisitor = relVisitor;
    this.rexVisitor = rexVisitor;
  }

  RelNode plan(Foreach node, CalcitePlanContext context) {
    return node.getMode() == Foreach.Mode.MULTIFIELD
        ? planMultifield(node, context)
        : planCollection(node, context);
  }

  // ---------------------------------------------------------------------- multifield

  private RelNode planMultifield(Foreach node, CalcitePlanContext context) {
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    Set<String> matchingFields = new LinkedHashSet<>();
    for (String pattern : node.getFieldPatterns()) {
      matchingFields.addAll(WildcardUtils.expandWildcardPattern(pattern, currentFields));
    }

    for (String fieldName : matchingFields) {
      ForeachBindings bindings =
          multifieldBindings(node.getFieldPatterns(), fieldName, node.getOptions());
      context.pushForeachBindings(bindings.values(), bindings.identifiers());
      try {
        for (ForeachEvalClause clause : node.getEvalClauses()) {
          RexNode expr = rexVisitor.analyze(clause.getExpression(), context);
          String alias = substituteTemplate(clause.getTargetTemplate(), bindings.values());
          relVisitor.projectPlusOverriding(
              List.of(context.relBuilder.alias(expr, alias)), List.of(alias), context);
        }
      } finally {
        context.clearForeachBindings();
      }
    }
    return context.relBuilder.peek();
  }

  private ForeachBindings multifieldBindings(
      List<String> patterns, String fieldName, Map<String, String> options) {
    Map<String, ForeachBinding> bindings = new LinkedHashMap<>();
    Map<String, ForeachBinding> identifiers = new LinkedHashMap<>();
    bindOption(
        bindings,
        identifiers,
        new ForeachBinding(fieldName, ForeachBindingType.FIELD),
        PLACEHOLDER_FIELD,
        OPTION_FIELDSTR,
        options);
    List<String> orderedPatterns =
        Stream.concat(
                patterns.stream()
                    .filter(pattern -> !WildcardUtils.containsWildcard(pattern))
                    .filter(pattern -> WildcardUtils.matchesWildcardPattern(pattern, fieldName)),
                patterns.stream().filter(WildcardUtils::containsWildcard))
            .toList();
    for (String pattern : orderedPatterns) {
      List<String> segments = wildcardSegments(pattern, fieldName);
      if (segments == null) {
        continue;
      }
      bindOption(
          bindings,
          identifiers,
          new ForeachBinding(String.join("", segments), ForeachBindingType.LITERAL),
          PLACEHOLDER_MATCHSTR,
          OPTION_MATCHSTR,
          options);
      for (int i = 0; i < segments.size(); i++) {
        String defaultName = PLACEHOLDER_MATCHSEG + (i + 1);
        bindOption(
            bindings,
            identifiers,
            new ForeachBinding(segments.get(i), ForeachBindingType.LITERAL),
            defaultName,
            OPTION_MATCHSEG + (i + 1),
            options);
      }
      break;
    }
    return new ForeachBindings(bindings, identifiers);
  }

  /**
   * Returns the substrings of {@code fieldName} captured by the wildcards in {@code pattern}, an
   * empty list if the pattern matches without wildcards, or null if it does not match.
   */
  private List<String> wildcardSegments(String pattern, String fieldName) {
    if (!WildcardUtils.matchesWildcardPattern(pattern, fieldName)) {
      return null;
    }
    if (!WildcardUtils.containsWildcard(pattern)) {
      return List.of();
    }
    Matcher matcher =
        Pattern.compile(WildcardUtils.convertWildcardPatternToRegex(pattern)).matcher(fieldName);
    if (!matcher.matches()) {
      return null;
    }
    List<String> segments = new ArrayList<>();
    for (int i = 1; i <= matcher.groupCount(); i++) {
      segments.add(matcher.group(i));
    }
    return segments;
  }

  // ---------------------------------------------------------------------- collection modes

  private RelNode planCollection(Foreach node, CalcitePlanContext context) {
    Foreach.Mode mode = node.getMode();
    UnresolvedExpression collection = node.getCollectionExpression();
    if (collection == null && mode == Foreach.Mode.AUTO_COLLECTIONS) {
      collection = firstArrayField(context);
    }
    if (collection == null) {
      throw new SemanticCheckException("foreach " + mode + " mode requires a field");
    }
    RexNode rawCollection = rexVisitor.analyze(collection, context);
    boolean nativeArray = rawCollection.getType() instanceof ArraySqlType;
    if ((mode == Foreach.Mode.MULTIVALUE && !nativeArray)
        || (mode == Foreach.Mode.JSON_ARRAY && nativeArray)) {
      return context.relBuilder.peek();
    }
    collection = asArrayExpression(collection, nativeArray, context, node);
    RexNode collectionRex = rexVisitor.analyze(collection, context);
    if (!(collectionRex.getType() instanceof ArraySqlType arrayType)) {
      return context.relBuilder.peek();
    }
    RelDataType itemType = arrayType.getComponentType();
    RelDataType iterType = context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    List<String> targetAliases =
        node.getEvalClauses().stream().map(ForeachEvalClause::getTargetTemplate).toList();
    List<RexNode> initialTargets =
        targetAliases.stream()
            .map(alias -> rexVisitor.analyze(new Field(new QualifiedName(alias)), context))
            .toList();
    List<RelDataTypeField> capturedFields =
        capturedFields(node.getEvalClauses(), context, node.getOptions(), targetAliases);

    // Pack [item, iter, captured...] so the reduce lambda can reach everything through one var.
    List<UnresolvedExpression> pairArgs = new ArrayList<>();
    pairArgs.add(collection);
    capturedFields.stream()
        .map(field -> new Field(new QualifiedName(field.getName())))
        .forEach(pairArgs::add);
    Function pairedCollection =
        new Function(
            BuiltinFunctionName.FOREACH_PAIR_COLLECTION.getName().getFunctionName(), pairArgs);
    RexNode pairedCollectionRex = rexVisitor.analyze(pairedCollection, context);

    List<RelDataType> targetTypes = initialTargets.stream().map(RexNode::getType).toList();
    List<RexNode> probedExpressions =
        analyzeCollectionEval(
            node,
            context,
            pairedCollectionRex,
            state(initialTargets, context),
            itemType,
            iterType,
            capturedFields,
            targetAliases,
            targetTypes);
    targetTypes = probedExpressions.stream().map(RexNode::getType).toList();
    List<RexNode> castInitialTargets = new ArrayList<>();
    for (int i = 0; i < initialTargets.size(); i++) {
      castInitialTargets.add(
          context.rexBuilder.makeCast(targetTypes.get(i), initialTargets.get(i), true, true));
    }
    RexNode initialState = state(castInitialTargets, context);

    ForeachBindings bindings =
        collectionBindings(node, itemType, iterType, capturedFields, targetAliases, targetTypes);
    context.stageForeachLambdaBindings(bindings.values(), bindings.identifiers());
    try {
      CalcitePlanContext lambdaContext =
          prepareStateLambdaContext(context, pairedCollectionRex, initialState);
      List<RexNode> updatedTargets = analyzeClauses(node.getEvalClauses(), lambdaContext);
      RexNode updatedState = state(updatedTargets, lambdaContext);
      RexNode lambda =
          context.rexBuilder.makeLambdaCall(
              updatedState,
              List.of(
                  lambdaContext.getRexLambdaRefMap().get(STATE_VAR),
                  lambdaContext.getRexLambdaRefMap().get(PAIR_VAR)));
      RexNode reducedState =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder,
              BuiltinFunctionName.REDUCE,
              pairedCollectionRex,
              initialState,
              lambda);
      List<RexNode> outputs = new ArrayList<>();
      for (int i = 0; i < targetAliases.size(); i++) {
        RexNode value = stateSlot(reducedState, i, targetTypes.get(i), context);
        outputs.add(context.relBuilder.alias(value, targetAliases.get(i)));
      }
      relVisitor.projectPlusOverriding(outputs, targetAliases, context);
    } finally {
      context.clearForeachBindings();
    }
    return context.relBuilder.peek();
  }

  private List<RexNode> analyzeCollectionEval(
      Foreach node,
      CalcitePlanContext context,
      RexNode pairedCollection,
      RexNode initialState,
      RelDataType itemType,
      RelDataType iterType,
      List<RelDataTypeField> capturedFields,
      List<String> targetAliases,
      List<RelDataType> targetTypes) {
    ForeachBindings bindings =
        collectionBindings(node, itemType, iterType, capturedFields, targetAliases, targetTypes);
    context.stageForeachLambdaBindings(bindings.values(), bindings.identifiers());
    try {
      return analyzeClauses(
          node.getEvalClauses(),
          prepareStateLambdaContext(context, pairedCollection, initialState));
    } finally {
      context.clearForeachBindings();
    }
  }

  private CalcitePlanContext prepareStateLambdaContext(
      CalcitePlanContext context, RexNode pairedCollection, RexNode initialState) {
    LambdaFunction template =
        new LambdaFunction(
            new Literal(0, DataType.INTEGER),
            List.of(new QualifiedName(STATE_VAR), new QualifiedName(PAIR_VAR)));
    return rexVisitor.prepareLambdaContext(
        context,
        template,
        List.of(pairedCollection, initialState),
        BuiltinFunctionName.REDUCE.getName().getFunctionName(),
        initialState.getType());
  }

  private List<RexNode> analyzeClauses(
      List<ForeachEvalClause> clauses, CalcitePlanContext lambdaContext) {
    List<RexNode> expressions = new ArrayList<>();
    for (ForeachEvalClause clause : clauses) {
      RexNode expression = rexVisitor.analyze(clause.getExpression(), lambdaContext);
      expressions.add(expression);
      lambdaContext.putForeachComputedBinding(clause.getTargetTemplate(), expression);
    }
    return expressions;
  }

  private RexNode state(List<RexNode> values, CalcitePlanContext context) {
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder, BuiltinFunctionName.FOREACH_STATE, values.toArray(RexNode[]::new));
  }

  private RexNode stateSlot(RexNode state, int slot, RelDataType type, CalcitePlanContext context) {
    return context.rexBuilder.makeCall(
        type,
        PPLBuiltinOperators.FOREACH_PAIR_ITEM,
        List.of(state, context.rexBuilder.makeExactLiteral(BigDecimal.valueOf(slot))));
  }

  private ForeachBindings collectionBindings(
      Foreach node,
      RelDataType itemType,
      RelDataType iterType,
      List<RelDataTypeField> capturedFields,
      List<String> targetAliases,
      List<RelDataType> targetTypes) {
    Map<String, ForeachBinding> bindings = new LinkedHashMap<>();
    Map<String, ForeachBinding> identifiers = new LinkedHashMap<>();
    bindPairPlaceholder(
        bindings, identifiers, 0, itemType, PLACEHOLDER_ITEM, OPTION_ITEMSTR, node.getOptions());
    bindPairPlaceholder(
        bindings, identifiers, 1, iterType, PLACEHOLDER_ITER, OPTION_ITERSTR, node.getOptions());
    for (int i = 0; i < capturedFields.size(); i++) {
      RelDataTypeField field = capturedFields.get(i);
      bindIdentifier(identifiers, field.getName(), PAIR_VAR, i + 2, field.getType());
    }
    for (int i = 0; i < targetAliases.size(); i++) {
      bindIdentifier(identifiers, targetAliases.get(i), STATE_VAR, i, targetTypes.get(i));
    }
    return new ForeachBindings(bindings, identifiers);
  }

  private void bindPairPlaceholder(
      Map<String, ForeachBinding> bindings,
      Map<String, ForeachBinding> identifiers,
      int slot,
      RelDataType type,
      String placeholder,
      String option,
      Map<String, String> options) {
    ForeachBinding binding = new ForeachBinding(PAIR_VAR, ForeachBindingType.PAIR_SLOT, slot, type);
    bindings.put(placeholder, binding);
    if (options.containsKey(option)) {
      String customName = options.get(option).toUpperCase(Locale.ROOT);
      bindings.put(customName, binding);
      identifiers.put(customName, binding);
    }
  }

  private void bindIdentifier(
      Map<String, ForeachBinding> identifiers,
      String name,
      String variable,
      int slot,
      RelDataType type) {
    String key = name.toUpperCase(Locale.ROOT);
    identifiers.put(key, new ForeachBinding(variable, ForeachBindingType.PAIR_SLOT, slot, type));
  }

  private UnresolvedExpression firstArrayField(CalcitePlanContext context) {
    return context.relBuilder.peek().getRowType().getFieldList().stream()
        .filter(field -> field.getType() instanceof ArraySqlType)
        .findFirst()
        .map(field -> (UnresolvedExpression) new Field(new QualifiedName(field.getName())))
        .orElseThrow(
            () ->
                new SemanticCheckException(
                    "foreach auto_collections mode requires a multivalue field or JSON array"));
  }

  /**
   * Ensures the collection expression evaluates to a Calcite array. Non-array expressions (JSON
   * array strings or {@code json_array()} calls) are wrapped in {@code foreach_json_array}, which
   * parses the JSON and casts every element to one inferred type.
   */
  private UnresolvedExpression asArrayExpression(
      UnresolvedExpression collection,
      boolean nativeArray,
      CalcitePlanContext context,
      Foreach node) {
    if (nativeArray) {
      return collection;
    }
    return new Function(
        BuiltinFunctionName.FOREACH_JSON_ARRAY.getName().getFunctionName(),
        List.of(
            collection,
            new Literal(jsonElementType(collection, context, node).name(), DataType.STRING)));
  }

  /**
   * Infers the element type a JSON array collection should be read as. JSON only distinguishes
   * numbers and strings here, so the answer is DOUBLE (gson parses JSON numbers as doubles) or
   * VARCHAR.
   *
   * <p>For {@code json_array()} calls and string literals the content is visible at plan time;
   * mixed content is rejected. For opaque expressions, typically a field holding JSON text, content
   * is unknowable, so infer from usage: an item placeholder consumed by arithmetic means numeric
   * elements, anything else means strings.
   */
  private SqlTypeName jsonElementType(
      UnresolvedExpression collection, CalcitePlanContext context, Foreach node) {
    if (collection instanceof Function function
        && BuiltinFunctionName.JSON_ARRAY
            .getName()
            .getFunctionName()
            .equalsIgnoreCase(function.getFuncName())) {
      return elementTypeOf(
          function.getFuncArgs().stream()
              .map(arg -> rexVisitor.analyze(arg, context).getType())
              .map(RelDataType::getFamily)
              .collect(Collectors.toSet()));
    }
    if (collection instanceof Literal literal && literal.getType() == DataType.STRING) {
      try {
        List<?> values = gson.fromJson(String.valueOf(literal.getValue()), List.class);
        if (values != null) {
          return elementTypeOf(
              values.stream()
                  .filter(Objects::nonNull)
                  .map(v -> v instanceof Number ? SqlTypeFamily.NUMERIC : SqlTypeFamily.CHARACTER)
                  .collect(Collectors.toSet()));
        }
      } catch (JsonSyntaxException ignored) {
        // Malformed JSON literals return null at runtime; type choice is irrelevant.
      }
      return SqlTypeName.VARCHAR;
    }
    return itemRequiresNumericType(node, context) ? SqlTypeName.DOUBLE : SqlTypeName.VARCHAR;
  }

  private boolean itemRequiresNumericType(Foreach node, CalcitePlanContext context) {
    String itemName = node.getOptions().getOrDefault(OPTION_ITEMSTR, PLACEHOLDER_ITEM);
    boolean customIdentifier = node.getOptions().containsKey(OPTION_ITEMSTR);
    return node.getEvalClauses().stream()
        .anyMatch(
            clause ->
                itemRequiresNumericType(
                    clause.getExpression(), itemName, customIdentifier, context));
  }

  private boolean itemRequiresNumericType(
      Node node, String itemName, boolean customIdentifier, CalcitePlanContext context) {
    if (node instanceof Compare compare) {
      if ((containsItem(compare.getLeft(), itemName, customIdentifier)
              && isNumericExpression(compare.getRight(), context))
          || (containsItem(compare.getRight(), itemName, customIdentifier)
              && isNumericExpression(compare.getLeft(), context))) {
        return true;
      }
    }
    if (node instanceof Function function) {
      for (int i = 0; i < function.getFuncArgs().size(); i++) {
        if (containsItem(function.getFuncArgs().get(i), itemName, customIdentifier)
            && (ARITHMETIC_OPERATORS.contains(function.getFuncName())
                || PPLFuncImpTable.INSTANCE.requiresNumericArgument(function.getFuncName(), i))) {
          return true;
        }
      }
    }
    return node.getChild().stream()
        .anyMatch(child -> itemRequiresNumericType(child, itemName, customIdentifier, context));
  }

  private boolean containsItem(Node node, String itemName, boolean customIdentifier) {
    if (isItemReference(node, itemName, customIdentifier)) {
      return true;
    }
    return node.getChild().stream()
        .anyMatch(child -> containsItem(child, itemName, customIdentifier));
  }

  private boolean isItemReference(Node node, String itemName, boolean customIdentifier) {
    String name;
    if (node instanceof ForeachPlaceholder placeholder) {
      name = placeholder.getName();
    } else if (node instanceof QualifiedName qualifiedName) {
      if (!customIdentifier) {
        return false;
      }
      name = qualifiedName.toString();
    } else {
      return false;
    }
    return PLACEHOLDER_ITEM.equalsIgnoreCase(name) || itemName.equalsIgnoreCase(name);
  }

  private boolean isNumericExpression(Node node, CalcitePlanContext context) {
    if (node instanceof Literal literal) {
      return Set.of(
              DataType.SHORT,
              DataType.INTEGER,
              DataType.LONG,
              DataType.FLOAT,
              DataType.DOUBLE,
              DataType.DECIMAL)
          .contains(literal.getType());
    }
    try {
      return rexVisitor.analyze((UnresolvedExpression) node, context).getType().getFamily()
          == SqlTypeFamily.NUMERIC;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private SqlTypeName elementTypeOf(Set<?> families) {
    boolean numeric = families.contains(SqlTypeFamily.NUMERIC);
    boolean character = families.contains(SqlTypeFamily.CHARACTER);
    if (numeric && character) {
      throw new SemanticCheckException(
          "foreach json_array elements must be consistently strings or numbers");
    }
    return numeric ? SqlTypeName.DOUBLE : SqlTypeName.VARCHAR;
  }

  /** Row fields referenced by the eval clauses that must ride along inside the pairs. */
  private List<RelDataTypeField> capturedFields(
      List<ForeachEvalClause> evalClauses,
      CalcitePlanContext context,
      Map<String, String> options,
      List<String> targetAliases) {
    Set<String> excluded =
        Stream.concat(Stream.of(PAIR_VAR), targetAliases.stream())
            .map(name -> name.toUpperCase(Locale.ROOT))
            .collect(Collectors.toSet());
    if (options.containsKey(OPTION_ITEMSTR)) {
      excluded.add(options.get(OPTION_ITEMSTR).toUpperCase(Locale.ROOT));
    }
    if (options.containsKey(OPTION_ITERSTR)) {
      excluded.add(options.get(OPTION_ITERSTR).toUpperCase(Locale.ROOT));
    }
    Map<String, RelDataTypeField> rowFields =
        context.relBuilder.peek().getRowType().getFieldList().stream()
            .collect(
                Collectors.toMap(
                    field -> field.getName().toUpperCase(Locale.ROOT),
                    field -> field,
                    (left, right) -> left,
                    LinkedHashMap::new));
    Map<String, RelDataTypeField> captured = new LinkedHashMap<>();
    evalClauses.forEach(
        clause -> collectFieldReferences(clause.getExpression(), excluded, rowFields, captured));
    return new ArrayList<>(captured.values());
  }

  private void collectFieldReferences(
      Node node,
      Set<String> excluded,
      Map<String, RelDataTypeField> rowFields,
      Map<String, RelDataTypeField> captured) {
    if (node instanceof QualifiedName qualifiedName) {
      String key = qualifiedName.toString().toUpperCase(Locale.ROOT);
      RelDataTypeField field = rowFields.get(key);
      if (field != null && !excluded.contains(key)) {
        captured.putIfAbsent(key, field);
      }
    }
    node.getChild().forEach(child -> collectFieldReferences(child, excluded, rowFields, captured));
  }

  // ---------------------------------------------------------------------- shared

  private void bindOption(
      Map<String, ForeachBinding> bindings,
      Map<String, ForeachBinding> identifiers,
      ForeachBinding binding,
      String defaultName,
      String option,
      Map<String, String> options) {
    bindings.put(defaultName.toUpperCase(Locale.ROOT), binding);
    if (options.containsKey(option)) {
      String customName = options.get(option).toUpperCase(Locale.ROOT);
      bindings.put(customName, binding);
      identifiers.put(customName, binding);
    }
  }

  private String substituteTemplate(String template, Map<String, ForeachBinding> bindings) {
    String result = template;
    for (Map.Entry<String, ForeachBinding> entry : bindings.entrySet()) {
      result =
          result.replaceAll(
              "(?i)<<" + Pattern.quote(entry.getKey()) + ">>",
              Matcher.quoteReplacement(entry.getValue().value()));
    }
    return result;
  }

  private record ForeachBindings(
      Map<String, ForeachBinding> values, Map<String, ForeachBinding> identifiers) {}
}

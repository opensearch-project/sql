/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.gson;

import com.google.gson.JsonSyntaxException;
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
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
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
      Map<String, ForeachBinding> bindings =
          multifieldBindings(node.getFieldPatterns(), fieldName, node.getOptions());
      context.pushForeachBindings(bindings);
      try {
        for (ForeachEvalClause clause : node.getEvalClauses()) {
          RexNode expr = rexVisitor.analyze(clause.getExpression(), context);
          String alias = substituteTemplate(clause.getTargetTemplate(), bindings);
          relVisitor.projectPlusOverriding(
              List.of(context.relBuilder.alias(expr, alias)), List.of(alias), context);
        }
      } finally {
        context.clearForeachBindings();
      }
    }
    return context.relBuilder.peek();
  }

  private Map<String, ForeachBinding> multifieldBindings(
      List<String> patterns, String fieldName, Map<String, String> options) {
    Map<String, ForeachBinding> bindings = new LinkedHashMap<>();
    bind(
        bindings,
        new ForeachBinding(fieldName, ForeachBindingType.FIELD),
        PLACEHOLDER_FIELD,
        options.getOrDefault(OPTION_FIELDSTR, PLACEHOLDER_FIELD));
    for (String pattern : patterns) {
      List<String> segments = wildcardSegments(pattern, fieldName);
      if (segments == null) {
        continue;
      }
      bind(
          bindings,
          new ForeachBinding(String.join("", segments), ForeachBindingType.LITERAL),
          PLACEHOLDER_MATCHSTR,
          options.getOrDefault(OPTION_MATCHSTR, PLACEHOLDER_MATCHSTR));
      for (int i = 0; i < segments.size(); i++) {
        String defaultName = PLACEHOLDER_MATCHSEG + (i + 1);
        bind(
            bindings,
            new ForeachBinding(segments.get(i), ForeachBindingType.LITERAL),
            defaultName,
            options.getOrDefault(OPTION_MATCHSEG + (i + 1), defaultName));
      }
      break;
    }
    return bindings;
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
    collection = asArrayExpression(collection, mode, context);
    RexNode collectionRex = rexVisitor.analyze(collection, context);
    if (!(collectionRex.getType() instanceof ArraySqlType arrayType)) {
      throw new SemanticCheckException("foreach " + mode + " mode requires a multivalue field");
    }
    RelDataType itemType = arrayType.getComponentType();
    RelDataType iterType = context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);

    String itemName = node.getOptions().getOrDefault(OPTION_ITEMSTR, PLACEHOLDER_ITEM);
    String iterName = node.getOptions().getOrDefault(OPTION_ITERSTR, PLACEHOLDER_ITER);
    List<String> targetAliases =
        node.getEvalClauses().stream().map(ForeachEvalClause::getTargetTemplate).toList();
    List<RelDataTypeField> capturedFields =
        capturedFields(node.getEvalClauses(), context, itemName, iterName, targetAliases);

    // Pack [item, iter, captured...] so the reduce lambda can reach everything through one var.
    List<UnresolvedExpression> pairArgs = new ArrayList<>();
    pairArgs.add(collection);
    capturedFields.stream()
        .map(field -> new Field(new QualifiedName(field.getName())))
        .forEach(pairArgs::add);
    Function pairedCollection =
        new Function(
            BuiltinFunctionName.FOREACH_PAIR_COLLECTION.getName().getFunctionName(), pairArgs);

    Map<String, ForeachBinding> bindings = new LinkedHashMap<>();
    bindPairSlot(bindings, 0, itemType, itemName, PLACEHOLDER_ITEM);
    bindPairSlot(bindings, 1, iterType, iterName, PLACEHOLDER_ITER);
    for (int i = 0; i < capturedFields.size(); i++) {
      RelDataTypeField field = capturedFields.get(i);
      bindPairSlot(bindings, i + 2, field.getType(), field.getName());
    }

    // Stage rather than activate: the pair collection and the accumulator's initial value are
    // resolved against the row; only the lambda body sees the placeholder bindings.
    context.stageForeachLambdaBindings(bindings);
    try {
      for (ForeachEvalClause clause : node.getEvalClauses()) {
        String alias = substituteTemplate(clause.getTargetTemplate(), bindings);
        Function reduce =
            new Function(
                BuiltinFunctionName.REDUCE.getName().getFunctionName(),
                List.of(
                    pairedCollection,
                    new Field(new QualifiedName(alias)),
                    new LambdaFunction(
                        clause.getExpression(),
                        List.of(new QualifiedName(alias), new QualifiedName(PAIR_VAR)))));
        RexNode expr = rexVisitor.analyze(reduce, context);
        relVisitor.projectPlusOverriding(
            List.of(context.relBuilder.alias(expr, alias)), List.of(alias), context);
      }
    } finally {
      context.clearForeachBindings();
    }
    return context.relBuilder.peek();
  }

  private void bindPairSlot(
      Map<String, ForeachBinding> bindings, int slot, RelDataType type, String... names) {
    ForeachBinding binding = new ForeachBinding(PAIR_VAR, ForeachBindingType.PAIR_SLOT, slot, type);
    for (String name : names) {
      bindings.put(name.toUpperCase(Locale.ROOT), binding);
    }
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
      UnresolvedExpression collection, Foreach.Mode mode, CalcitePlanContext context) {
    if (mode == Foreach.Mode.MULTIVALUE
        || (mode == Foreach.Mode.AUTO_COLLECTIONS
            && rexVisitor.analyze(collection, context).getType() instanceof ArraySqlType)) {
      return collection;
    }
    return new Function(
        BuiltinFunctionName.FOREACH_JSON_ARRAY.getName().getFunctionName(),
        List.of(
            collection, new Literal(jsonElementType(collection, context).name(), DataType.STRING)));
  }

  /**
   * Infers the element type a JSON array collection should be read as. JSON only distinguishes
   * numbers and strings here, so the answer is DOUBLE (gson parses JSON numbers as doubles) or
   * VARCHAR. Mixed content is rejected; expressions whose content is unknowable at plan time (e.g.
   * a field holding a JSON string) default to VARCHAR.
   */
  private SqlTypeName jsonElementType(UnresolvedExpression collection, CalcitePlanContext context) {
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
    }
    return SqlTypeName.VARCHAR;
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
      String itemName,
      String iterName,
      List<String> targetAliases) {
    Set<String> excluded =
        Stream.concat(
                Stream.of(itemName, iterName, PLACEHOLDER_ITEM, PLACEHOLDER_ITER, PAIR_VAR),
                targetAliases.stream())
            .map(name -> name.toUpperCase(Locale.ROOT))
            .collect(Collectors.toSet());
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

  private void bind(
      Map<String, ForeachBinding> bindings,
      ForeachBinding binding,
      String defaultName,
      String customName) {
    bindings.put(defaultName.toUpperCase(Locale.ROOT), binding);
    bindings.put(customName.toUpperCase(Locale.ROOT), binding);
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
}

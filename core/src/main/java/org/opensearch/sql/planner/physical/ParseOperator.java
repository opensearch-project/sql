/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.planner.logical.LogicalParse.typeStrToExprType;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.operator.convert.TypeCastOperator;

/**
 * ParseOperator.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class ParseOperator extends PhysicalPlan {
  private static final Logger log = LogManager.getLogger(ParseOperator.class);
  private final PhysicalPlan input;
  private final Expression expression;
  private final String rawPattern;
  @EqualsAndHashCode.Exclude
  private final Pattern pattern;
  @EqualsAndHashCode.Exclude
  private final Map<String, String> groups;

  private static final BuiltinFunctionRepository REPOSITORY;

  static {
    REPOSITORY = new BuiltinFunctionRepository(new HashMap<>());
    TypeCastOperator.register(REPOSITORY);
  }

  /**
   * ParseOperator.
   */
  public ParseOperator(PhysicalPlan input,
                       Expression expression, String pattern, Map<String, String> groups) {
    this.input = input;
    this.expression = expression;
    this.rawPattern = pattern;
    this.pattern = Pattern.compile(rawPattern);
    this.groups = groups;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitParse(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public ExprValue next() {
    ExprValue inputValue = input.next();
    if (STRUCT != inputValue.type()) {
      return inputValue;
    }

    ExprValue value = inputValue.bindingTuples().resolve(expression);
    if (value.isNull() || value.isMissing()) {
      return inputValue;
    }

    Map<String, ExprValue> exprValueMap;
    try {
      exprValueMap = parse(value.stringValue());
    } catch (ExpressionEvaluationException e) {
      throw new SemanticCheckException(
          String.format("failed to parse field \"%s\" with type [%s]", expression, value.type()));
    }

    ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
    Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
    for (Entry<String, ExprValue> valueEntry : tupleValue.entrySet()) {
      if (exprValueMap.containsKey(valueEntry.getKey())) {
        resultBuilder.put(valueEntry.getKey(), exprValueMap.get(valueEntry.getKey()));
        exprValueMap.remove(valueEntry.getKey());
      } else {
        resultBuilder.put(valueEntry);
      }
    }
    resultBuilder.putAll(exprValueMap);
    return ExprTupleValue.fromExprValueMap(resultBuilder.build());
  }

  private Map<String, ExprValue> parse(String rawString) {
    Matcher matcher = pattern.matcher(rawString);
    Map<String, ExprValue> exprValueMap = new LinkedHashMap<>();
    if (matcher.matches()) {
      groups.forEach((field, type) -> {
        String rawMatch = matcher.group(field + type);
        try {
          Expression expression = REPOSITORY.cast(DSL.literal(rawMatch), typeStrToExprType(type));
          exprValueMap.put(field, expression.valueOf(null));
        } catch (SemanticCheckException | NumberFormatException e) {
          throw new SemanticCheckException(
              String.format("failed to cast \"%s\" to type [%s]", rawMatch, type));
        }
      });
    } else {
      log.warn("failed to extract pattern {} from input {}", rawPattern, rawString);
    }
    return exprValueMap;
  }
}

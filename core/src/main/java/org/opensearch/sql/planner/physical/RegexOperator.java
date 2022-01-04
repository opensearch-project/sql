package org.opensearch.sql.planner.physical;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.operator.convert.TypeCastOperator;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.sql.planner.logical.LogicalRegex.regexTypeToExprType;

/**
 * RegexOperator.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class RegexOperator extends PhysicalPlan {
  private static final Logger log = LogManager.getLogger(RegexOperator.class);
  /**
   * Input Plan.
   */
  @Getter
  private final PhysicalPlan input;
  /**
   * Expression.
   */
  @Getter
  private final Expression expression;
  /**
   * Raw Pattern.
   */
  @Getter
  private final String rawPattern;
  /**
   * Pattern.
   */
  @Getter
  private final Pattern pattern;

  @Getter
  private final Map<String, String> groups;

  private static final BuiltinFunctionRepository REPOSITORY;

  static {
    REPOSITORY = new BuiltinFunctionRepository(new HashMap<>());
    TypeCastOperator.register(REPOSITORY);
  }

  /**
   * RegexOperator.
   */
  public RegexOperator(PhysicalPlan input,
                       Expression expression, String pattern, Map<String, String> groups) {
    this.input = input;
    this.expression = expression;
    this.rawPattern = pattern;
    this.pattern = Pattern.compile(rawPattern);
    this.groups = groups;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRegex(this, context);
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

    ExprValue value = inputValue.bindingTuples().resolve(expression);
    final String rawString = value.stringValue();

    Matcher matcher = pattern.matcher(rawString);
    Map<String, ExprValue> exprValueMap = new LinkedHashMap<>();
    if (matcher.matches()) {
      groups.forEach((group, type) -> {
        Expression expression = REPOSITORY.cast(DSL.literal(matcher.group(group + type)), regexTypeToExprType(type));
        exprValueMap.put(group, expression.valueOf(null));
      });
    } else {
      log.warn("failed to extract pattern {} from input {}", rawPattern, rawString);
    }
    return ExprTupleValue.fromExprValueMap(exprValueMap);
  }
}

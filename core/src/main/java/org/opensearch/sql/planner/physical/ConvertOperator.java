/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.env.Environment.extendEnv;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

/**
 * The convert operator evaluates conversion expressions and applies type conversions to fields.
 *
 * <p>Similar to {@link EvalOperator}, this operator processes {@link
 * ConvertOperator#conversionList} from left to right, allowing references to previously converted
 * fields.
 *
 * <p>Example: convert auto(age), num(price) AS numeric_price
 *
 * <p>The operator will:
 *
 * <ul>
 *   <li>Apply the auto() conversion function to the age field
 *   <li>Apply the num() conversion function to price and store as numeric_price
 * </ul>
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class ConvertOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;
  @Getter private final List<Pair<ReferenceExpression, Expression>> conversionList;
  @Getter private final String timeformat;

  /**
   * Constructor for ConvertOperator.
   *
   * @param input Input physical plan
   * @param conversionList List of conversion expressions to apply
   * @param timeformat Optional time format string for time conversions
   */
  public ConvertOperator(
      PhysicalPlan input,
      List<Pair<ReferenceExpression, Expression>> conversionList,
      String timeformat) {
    this.input = input;
    this.conversionList = conversionList;
    this.timeformat = timeformat;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitConvert(this, context);
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
    Map<String, ExprValue> convertMap = convert(inputValue.bindingTuples());

    if (STRUCT == inputValue.type()) {
      ImmutableMap.Builder<String, ExprValue> resultBuilder = new Builder<>();
      Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);

      // Process existing fields, replacing with converted values if present
      for (Entry<String, ExprValue> valueEntry : tupleValue.entrySet()) {
        if (convertMap.containsKey(valueEntry.getKey())) {
          resultBuilder.put(valueEntry.getKey(), convertMap.get(valueEntry.getKey()));
          convertMap.remove(valueEntry.getKey());
        } else {
          resultBuilder.put(valueEntry);
        }
      }

      // Add any new fields from conversions
      resultBuilder.putAll(convertMap);
      return ExprTupleValue.fromExprValueMap(resultBuilder.build());
    } else {
      return inputValue;
    }
  }

  /**
   * Evaluate the conversion expressions in {@link ConvertOperator#conversionList}.
   *
   * @param env Environment containing current field values
   * @return Map of field names to converted ExprValues
   */
  protected Map<String, ExprValue> convert(Environment<Expression, ExprValue> env) {
    Map<String, ExprValue> convertResultMap = new LinkedHashMap<>();
    for (Pair<ReferenceExpression, Expression> pair : conversionList) {
      ReferenceExpression var = pair.getKey();
      ExprValue value = pair.getValue().valueOf(env);
      env = extendEnv(env, var, value);
      convertResultMap.put(var.toString(), value);
    }
    return convertResultMap;
  }
}

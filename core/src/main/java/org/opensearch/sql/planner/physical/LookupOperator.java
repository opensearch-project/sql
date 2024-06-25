/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

/** Lookup operator. Perform lookup on another OpenSearch index and enrich the results. */
@Getter
@EqualsAndHashCode(callSuper = false)
public class LookupOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;
  @Getter private final String indexName;
  @Getter private final Map<ReferenceExpression, ReferenceExpression> matchFieldMap;
  @Getter private final Map<ReferenceExpression, ReferenceExpression> copyFieldMap;
  @Getter private final Boolean appendOnly;
  private final BiFunction<String, Map<String, Object>, Map<String, Object>> lookup;

  /** Lookup Constructor. */
  @NonNull
  public LookupOperator(
      PhysicalPlan input,
      String indexName,
      Map<ReferenceExpression, ReferenceExpression> matchFieldMap,
      Boolean appendOnly,
      Map<ReferenceExpression, ReferenceExpression> copyFieldMap,
      BiFunction<String, Map<String, Object>, Map<String, Object>> lookup) {
    this.input = input;
    this.indexName = indexName;
    this.matchFieldMap = matchFieldMap;
    this.appendOnly = appendOnly;
    this.copyFieldMap = copyFieldMap;
    this.lookup = lookup;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitLookup(this, context);
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

    if (STRUCT == inputValue.type()) {
      Map<String, Object> matchMap = new HashMap<>();
      Map<String, Object> finalMap = new HashMap<>();

      for (Map.Entry<ReferenceExpression, ReferenceExpression> matchField :
          matchFieldMap.entrySet()) {
        Object val = inputValue.bindingTuples().resolve(matchField.getValue()).value();
        if (val != null) {
          matchMap.put(matchField.getKey().toString(), val);
        } else {
          // No value in search results, so we just return the input
          return inputValue;
        }
      }

      finalMap.put("_match", matchMap);

      Map<String, String> copyMap = new HashMap<>();

      if (!copyFieldMap.isEmpty()) {

        for (Map.Entry<ReferenceExpression, ReferenceExpression> copyField :
            copyFieldMap.entrySet()) {
          copyMap.put(String.valueOf(copyField.getKey()), String.valueOf(copyField.getValue()));
        }

        finalMap.put("_copy", copyMap.keySet());
      }

      Map<String, Object> source = lookup.apply(indexName, finalMap);

      if (source == null || source.isEmpty()) {
        // no lookup found or lookup is empty, so we just return the original input value
        return inputValue;
      }

      Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
      Map<String, ExprValue> resultBuilder = new HashMap<>();
      resultBuilder.putAll(tupleValue);

      if (appendOnly) {

        for (Map.Entry<String, Object> sourceField : source.entrySet()) {
          String u = copyMap.get(sourceField.getKey());
          resultBuilder.putIfAbsent(
              u == null ? sourceField.getKey() : u.toString(),
              ExprValueUtils.fromObjectValue(sourceField.getValue()));
        }
      } else {
        // default

        for (Map.Entry<String, Object> sourceField : source.entrySet()) {
          String u = copyMap.get(sourceField.getKey());
          resultBuilder.put(
              u == null ? sourceField.getKey() : u.toString(),
              ExprValueUtils.fromObjectValue(sourceField.getValue()));
        }
      }

      return ExprTupleValue.fromExprValueMap(resultBuilder);

    } else {
      return inputValue;
    }
  }
}

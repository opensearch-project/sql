/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

/** Lookup operator. Perform lookup on another OpenSearch index and enrich the results. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class LookupOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;
  @Getter private final String indexName;
  @Getter private final Map<ReferenceExpression, ReferenceExpression> matchFieldMap;
  @Getter private final Map<ReferenceExpression, ReferenceExpression> copyFieldMap;
  @Getter private final Boolean appendOnly;

  @EqualsAndHashCode.Exclude
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

      Map<String, Object> lookupResult = lookup.apply(indexName, finalMap);

      if (lookupResult == null || lookupResult.isEmpty()) {
        // no lookup found or lookup is empty, so we just return the original input value
        return inputValue;
      }

      Map<String, ExprValue> tupleInputValue = ExprValueUtils.getTupleValue(inputValue);
      Map<String, ExprValue> resultTupleBuilder = new LinkedHashMap<>();
      resultTupleBuilder.putAll(tupleInputValue);
      for (Map.Entry<String, Object> sourceOfAdditionalField : lookupResult.entrySet()) {
        String lookedUpFieldName = sourceOfAdditionalField.getKey();
        Object lookedUpFieldValue = sourceOfAdditionalField.getValue();
        String finalFieldName = copyMap.getOrDefault(lookedUpFieldName, lookedUpFieldName);
        ExprValue value = ExprValueUtils.fromObjectValue(lookedUpFieldValue);
        if (appendOnly) {
          resultTupleBuilder.putIfAbsent(finalFieldName, value);
        } else {
          resultTupleBuilder.put(finalFieldName, value);
        }
      }

      return ExprTupleValue.fromExprValueMap(resultTupleBuilder);

    } else {
      return inputValue;
    }
  }
}

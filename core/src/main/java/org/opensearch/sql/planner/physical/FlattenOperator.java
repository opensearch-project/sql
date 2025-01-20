/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

@Getter
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class FlattenOperator extends PhysicalPlan {

  private final PhysicalPlan input;
  private final ReferenceExpression field;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitFlatten(this, context);
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

    if (!hasNext()) {
      throw new NoSuchElementException("The next expression value does not exist");
    }

    String fieldName = field.getAttr();

    // Verify that the field name is valid.
    Map<String, ExprValue> exprValueForFieldNameMap = ExprValueUtils.getTupleValue(input.next());
    if (!exprValueForFieldNameMap.containsKey(fieldName)) {
      throw new IllegalArgumentException(
          String.format("Field name '%s' for flatten command is not valid", fieldName));
    }

    // Verify that the field is a tuple.
    ExprValue exprValue = exprValueForFieldNameMap.get(fieldName);
    if (exprValue.type() != STRUCT) {
      throw new IllegalArgumentException(
          String.format("Field '%s' for flatten command must be a struct", fieldName));
    }

    // Flatten the tuple and add the flattened field names and values to result.
    Map<String, ExprValue> flattenedExprValueMap = flattenExprValue(exprValue);
    exprValueForFieldNameMap.putAll(flattenedExprValueMap);

    return ExprTupleValue.fromExprValueMap(exprValueForFieldNameMap);
  }

  /** Flattens the given tuple and returns the result. */
  private static Map<String, ExprValue> flattenExprValue(ExprValue exprValue) {

    ImmutableMap.Builder<String, ExprValue> flattenedMap = new ImmutableMap.Builder<>();

    for (Entry<String, ExprValue> entry : exprValue.tupleValue().entrySet()) {
      ExprValue entryExprValue = entry.getValue();

      // If the expression is a tuple, recursively flatten it.
      Map<String, ExprValue> flattenedEntryMap =
              (entryExprValue.type() == STRUCT)
                      ? flattenExprValue(entryExprValue)
                      : Map.of(entry.getKey(), entryExprValue);

      flattenedEntryMap.forEach(flattenedMap::put);
    }

    return flattenedMap.build();
  }
}

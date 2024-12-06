/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static java.util.Collections.singletonList;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

/** Remove the fields specified in {@link RemoveOperator#removeList} from input. */
@ToString
@EqualsAndHashCode(callSuper = false)
public class RemoveOperator extends PhysicalPlan {
  @Getter private final PhysicalPlan input;
  @Getter private final Set<ReferenceExpression> removeList;
  @ToString.Exclude @EqualsAndHashCode.Exclude private final Set<String> nameRemoveList;

  /**
   * Todo. This is the temporary solution that add the mapping between string and ref. because when
   * rename the field from input, there we can only get the string field.
   */
  public RemoveOperator(PhysicalPlan input, Set<ReferenceExpression> removeList) {
    this.input = input;
    this.removeList = removeList;
    this.nameRemoveList =
        removeList.stream().map(ReferenceExpression::getAttr).collect(Collectors.toSet());
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRemove(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public ExprValue next() {
    ExprValue inputValue = input.next();
    if (STRUCT == inputValue.type()) {
      ImmutableMap.Builder<String, ExprValue> mapBuilder = new Builder<>();
      Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
      for (Entry<String, ExprValue> valueEntry : tupleValue.entrySet()) {
        if (!nameRemoveList.contains(valueEntry.getKey())) {
          mapBuilder.put(valueEntry);
        }
      }
      return ExprTupleValue.fromExprValueMap(mapBuilder.build());
    } else {
      return inputValue;
    }
  }
}

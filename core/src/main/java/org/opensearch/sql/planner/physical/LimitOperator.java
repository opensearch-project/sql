/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.planner.SerializablePlan;

/**
 * The limit operator sets a window, to and block the rows out of the window
 * and allow only the result subset within this window to the output.
 *
 * <p>The result subset is enframed from original result with {@link LimitOperator#offset}
 * as the offset and {@link LimitOperator#limit} as the size, thus the output
 * is the subset of the original result set that has indices from {index + 1} to {index + limit}.
 * Special cases might occur where the result subset has a size smaller than expected {limit},
 * it occurs when the original result set has a size smaller than {index + limit},
 * or even not greater than the offset. The latter results in an empty output.</p>
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class LimitOperator extends PhysicalPlan implements SerializablePlan {
  private PhysicalPlan input;
  private Integer limit;
  private Integer offset;
  private Integer count = 0;

  public LimitOperator(PhysicalPlan input, Integer limit, Integer offset) {
    this.input = input;
    this.limit = limit;
    this.offset = offset;
  }

  @Override
  public void open() {
    super.open();

    // skip the leading rows of offset size
    while (input.hasNext() && count < offset) {
      count++;
      input.next();
    }
  }

  @Override
  public boolean hasNext() {
    return input.hasNext() && count < offset + limit;
  }

  @Override
  public ExprValue next() {
    count++;
    return input.next();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitLimit(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of(input);
  }

  /** Don't use, it is for deserialization needs only. */
  @Deprecated
  public LimitOperator() {
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    limit = in.readInt();
    count = in.readInt();
    // note: offset aren't serialized and deserialized, because not supported in pagination
    // TODO open a GH ticket and post here link
    offset = 0;
    input = (PhysicalPlan) in.readObject();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (count == limit) {
      // paging is finished
      throw new NoCursorException();
    }
    out.writeInt(limit);
    out.writeInt(count);
    out.writeObject(((SerializablePlan) input).getPlanForSerialization());
  }
}

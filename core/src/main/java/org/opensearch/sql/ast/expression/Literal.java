/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node of literal type Params include literal value (@value) and literal data type
 * (@type) which can be selected from {@link DataType}.
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class Literal extends UnresolvedExpression {

  private final Object value;
  private final DataType type;

  public Literal(Object value, DataType dataType) {
    if (dataType == DataType.DECIMAL && value instanceof Double) {
      this.value = BigDecimal.valueOf((Double) value);
    } else {
      this.value = value;
    }
    this.type = dataType;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitLiteral(this, context);
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}

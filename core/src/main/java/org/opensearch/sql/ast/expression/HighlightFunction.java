package org.opensearch.sql.ast.expression;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
@ToString
public class HighlightFunction extends UnresolvedExpression {
  @Getter
  private final String highlightField;


  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitHighlight(this, context);
  }
}

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class HighlightFunction extends UnresolvedExpression {
  private final UnresolvedExpression highlightField;
  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitHighlight(this, context);
  }

  public HighlightFunction(UnresolvedExpression field) {
    this.highlightField = field;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return List.of(highlightField);
  }
}

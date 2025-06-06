/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/** Argument. */
@Getter
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Argument extends UnresolvedExpression {
  private final String argName;
  private final Literal value;

  //    private final DataType valueType;
  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(value);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitArgument(this, context);
  }

  /** ArgumentMap is a helper class to get argument value by name. */
  public static class ArgumentMap {
    private final Map<String, Literal> map;

    public ArgumentMap(List<Argument> arguments) {
      this.map =
          arguments.stream()
              .collect(java.util.stream.Collectors.toMap(Argument::getArgName, Argument::getValue));
    }

    public static ArgumentMap of(List<Argument> arguments) {
      return new ArgumentMap(arguments);
    }

    /**
     * Get argument value by name.
     *
     * @param name argument name
     * @return argument value
     */
    public Literal get(String name) {
      return map.get(name);
    }
  }
}

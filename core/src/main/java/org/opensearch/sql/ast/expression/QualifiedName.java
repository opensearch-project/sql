/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.AbstractNodeVisitor;

@Getter
@EqualsAndHashCode(callSuper = false)
public class QualifiedName extends UnresolvedExpression {
  private final List<String> parts;

  public QualifiedName(String name) {
    this.parts = Collections.singletonList(name);
  }

  /**
   * QualifiedName Constructor.
   */
  public QualifiedName(Iterable<String> parts) {
    List<String> partsList = StreamSupport.stream(parts.spliterator(), false).collect(toList());
    if (partsList.isEmpty()) {
      throw new IllegalArgumentException("parts is empty");
    }
    this.parts = partsList;
  }

  /**
   * Construct {@link QualifiedName} from list of string.
   */
  public static QualifiedName of(String first, String... rest) {
    requireNonNull(first);
    ArrayList<String> parts = new ArrayList<>();
    parts.add(first);
    parts.addAll(Arrays.asList(rest));
    return new QualifiedName(parts);
  }

  public static QualifiedName of(Iterable<String> parts) {
    return new QualifiedName(parts);
  }

  /**
   * Get Prefix of {@link QualifiedName}.
   */
  public Optional<QualifiedName> getPrefix() {
    if (parts.size() == 1) {
      return Optional.empty();
    }
    return Optional.of(QualifiedName.of(parts.subList(0, parts.size() - 1)));
  }

  public String getSuffix() {
    return parts.get(parts.size() - 1);
  }

  /**
   * Get first part of the qualified name.
   * @return  first part
   */
  public Optional<String> first() {
    if (parts.size() == 1) {
      return Optional.empty();
    }
    return Optional.of(parts.get(0));
  }

  /**
   * Get rest parts of the qualified name. Assume that there must be remaining parts
   * so caller is responsible for the check (first() or size() must be called first).<br>
   * For example:<br>
   * {@code<br>
   *  &ensp; QualifiedName name = ...<br>
   *  &ensp; Optional<String> first = name.first();<br>
   *  &ensp; if (first.isPresent()) {<br>
   *  &emsp;   name.rest() ...<br>
   *  &ensp; }<br>
   * }
   * @return  rest part(s)
   */
  public QualifiedName rest() {
    return QualifiedName.of(parts.subList(1, parts.size()));
  }

  public String toString() {
    return String.join(".", this.parts);
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitQualifiedName(this, context);
  }
}

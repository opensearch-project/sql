/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import java.util.Objects;
import org.opensearch.sql.legacy.parser.ChildrenType;
import org.opensearch.sql.legacy.parser.NestedType;

/**
 * @author ansj
 */
public class Field implements Cloneable {

  /** Constant for '*' field in SELECT */
  public static final Field STAR = new Field("*", "");

  protected String name;
  protected SQLAggregateOption option;
  private String alias;
  private NestedType nested;
  private ChildrenType children;
  private SQLExpr expression;

  public Field(String name, String alias) {
    this.name = name;
    this.alias = alias;
    this.nested = null;
    this.children = null;
    this.option = null;
  }

  public Field(String name, String alias, NestedType nested, ChildrenType children) {
    this.name = name;
    this.alias = alias;
    this.nested = nested;
    this.children = children;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public boolean isNested() {
    return this.nested != null;
  }

  public boolean isReverseNested() {
    return this.nested != null && this.nested.isReverse();
  }

  public void setNested(NestedType nested) {
    this.nested = nested;
  }

  public String getNestedPath() {
    if (this.nested == null) {
      return null;
    }

    return this.nested.path;
  }

  public boolean isChildren() {
    return this.children != null;
  }

  public void setChildren(ChildrenType children) {
    this.children = children;
  }

  public String getChildType() {
    if (this.children == null) {
      return null;
    }
    return this.children.childType;
  }

  public void setAggregationOption(SQLAggregateOption option) {
    this.option = option;
  }

  public SQLAggregateOption getOption() {
    return option;
  }

  @Override
  public String toString() {
    return this.name;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj.getClass() != this.getClass()) {
      return false;
    }
    Field other = (Field) obj;
    boolean namesAreEqual =
        (other.getName() == null && this.name == null) || other.getName().equals(this.name);
    if (!namesAreEqual) {
      return false;
    }
    return (other.getAlias() == null && this.alias == null) || other.getAlias().equals(this.alias);
  }

  @Override
  public int hashCode() { // Bug: equals() is present but hashCode was missing
    return Objects.hash(name, alias);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new Field(new String(this.name), new String(this.alias));
  }

  /** Returns true if the field is script field. */
  public boolean isScriptField() {
    return false;
  }

  public void setExpression(SQLExpr expression) {
    this.expression = expression;
  }

  public SQLExpr getExpression() {
    return expression;
  }
}

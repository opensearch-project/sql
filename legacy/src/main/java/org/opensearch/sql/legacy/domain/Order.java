/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

/**
 * @author ansj
 */
public class Order {
  private String nestedPath;
  private String name;
  private String type;
  private Field sortField;

  public boolean isScript() {
    return sortField != null && sortField.isScriptField();
  }

  public Order(String nestedPath, String name, String type, Field sortField) {
    this.nestedPath = nestedPath;
    this.name = name;
    this.type = type;
    this.sortField = sortField;
  }

  public String getNestedPath() {
    return nestedPath;
  }

  public void setNestedPath(String nestedPath) {
    this.nestedPath = nestedPath;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Field getSortField() {
    return sortField;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.builder;

import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Item list
 */
public class ListItems {
  private final StringBuilder list = new StringBuilder();
  private int index = 0;

  public void addItem(String text) {
    list.append(index()).append(text).append('\n');
  }

  private String index() {
    index++;
    return StringUtils.format("%d. ", index);
  }

  @Override
  public String toString() {
    return list.toString();
  }
}

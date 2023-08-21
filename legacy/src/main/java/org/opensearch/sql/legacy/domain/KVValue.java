/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

public class KVValue implements Cloneable {
  public String key;
  public Object value;

  public KVValue(Object value) {
    this.value = value;
  }

  public KVValue(String key, Object value) {
    if (key != null) {
      this.key = key.replace("'", "");
    }
    this.value = value;
  }

  @Override
  public String toString() {
    if (key == null) {
      return value.toString();
    } else {
      return key + "=" + value;
    }
  }
}

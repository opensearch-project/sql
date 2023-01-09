/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class Cursor {
  public static final Cursor None = new Cursor();

  @Getter
  private final byte[] raw ;
  private Cursor() {
    raw = new byte[] {};
  }

  public Cursor(byte[] raw) {
    this.raw = raw;
  }

  @Override
  public String toString() {
    return new String(raw);
  }
}

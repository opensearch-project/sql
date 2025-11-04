/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@RequiredArgsConstructor
public class LimitDigest {
  private final int limit;
  private final int offset;

  public int limit() {
    return limit;
  }

  public int offset() {
    return offset;
  }

  @Override
  public String toString() {
    return offset == 0 ? String.valueOf(limit) : "[" + limit + " from " + offset + "]";
  }
}

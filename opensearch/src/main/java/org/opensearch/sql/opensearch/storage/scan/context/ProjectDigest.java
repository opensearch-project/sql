/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.List;

public record ProjectDigest(List<String> names, List<Integer> selectedColumns) {
  @Override
  public String toString() {
    return names.toString();
  }
}

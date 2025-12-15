/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.List;
import org.opensearch.search.sort.SortBuilder;

public record SortDigest(List<SortBuilder<?>> builders) {
  @Override
  public String toString() {
    return builders.toString();
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class ProjectDigest {
  private final List<String> names;
  private final List<Integer> selectedColumns;

  public List<String> names() {
    return names;
  }

  public List<Integer> selectedColumns() {
    return selectedColumns;
  }

  public String toString() {
    return names.toString();
  }
}

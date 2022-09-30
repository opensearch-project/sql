/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.analysis.model;

import java.util.List;
import lombok.Getter;

@Getter
public class SchemaName {

  public static final String DEFAULT_SCHEMA_NAME = "default";
  public static final String INFORMATION_SCHEMA_NAME = "information_schema";
  private String name = DEFAULT_SCHEMA_NAME;

  /**
   * Capture only if there are more parts in the name.
   *
   * @param parts parts.
   * @return remaining parts.
   */
  List<String> capture(List<String> parts) {
    if (parts.size() > 1
        && (DEFAULT_SCHEMA_NAME.equals(parts.get(0))
        || INFORMATION_SCHEMA_NAME.contains(parts.get(0)))) {
      name = parts.get(0);
      return parts.subList(1, parts.size());
    } else {
      return parts;
    }
  }

}

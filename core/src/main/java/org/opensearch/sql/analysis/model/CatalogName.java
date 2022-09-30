/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.analysis.model;

import java.util.List;
import java.util.Set;
import lombok.Getter;

@Getter
public class CatalogName {

  public static final String DEFAULT_CATALOG_NAME = ".opensearch";
  private String name = DEFAULT_CATALOG_NAME;

  /**
   * Capture only if there are more parts in the name.
   *
   * @param parts           parts.
   * @param allowedCatalogs allowedCatalogs.
   * @return remaining parts.
   */
  List<String> capture(List<String> parts, Set<String> allowedCatalogs) {
    if (parts.size() > 1 && allowedCatalogs.contains(parts.get(0))) {
      name = parts.get(0);
      return parts.subList(1, parts.size());
    } else {
      return parts;
    }
  }

}

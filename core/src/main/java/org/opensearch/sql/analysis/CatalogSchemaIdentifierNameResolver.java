/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.analysis;

import java.util.List;
import java.util.Set;

public class CatalogSchemaIdentifierNameResolver {

  public static final String DEFAULT_CATALOG_NAME = "@opensearch";
  public static final String DEFAULT_SCHEMA_NAME = "default";
  public static final String INFORMATION_SCHEMA_NAME = "information_schema";

  private String catalogName = DEFAULT_CATALOG_NAME;
  private String schemaName = DEFAULT_SCHEMA_NAME;
  private String identifierName;

  private static final String DOT = ".";

  /**
   * Data model for capturing catalog, schema and identifier from
   * fully qualifiedName. In the current state, it is used to capture
   * CatalogSchemaTable name and CatalogSchemaFunction in case of table
   * functions.
   *
   * @param parts           parts of qualifiedName.
   * @param allowedCatalogs allowedCatalogs.
   */
  public CatalogSchemaIdentifierNameResolver(List<String> parts, Set<String> allowedCatalogs) {
    List<String> remainingParts = captureSchemaName(captureCatalogName(parts, allowedCatalogs));
    identifierName = String.join(DOT, remainingParts);
  }

  public String getIdentifierName() {
    return identifierName;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public String getSchemaName() {
    return schemaName;
  }


  // Capture catalog name and return remaining parts(schema name and table name)
  // from the fully qualified name.
  private List<String> captureCatalogName(List<String> parts, Set<String> allowedCatalogs) {
    if (parts.size() > 1 && allowedCatalogs.contains(parts.get(0))
        || DEFAULT_CATALOG_NAME.equals(parts.get(0))) {
      catalogName = parts.get(0);
      return parts.subList(1, parts.size());
    } else {
      return parts;
    }
  }

  // Capture schema name and return the remaining parts(table name )
  // in the fully qualified name.
  private List<String> captureSchemaName(List<String> parts) {
    if (parts.size() > 1
        && (DEFAULT_SCHEMA_NAME.equals(parts.get(0))
        || INFORMATION_SCHEMA_NAME.contains(parts.get(0)))) {
      schemaName = parts.get(0);
      return parts.subList(1, parts.size());
    } else {
      return parts;
    }
  }


}

/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.analysis.model;

import java.util.List;
import java.util.Set;
import lombok.EqualsAndHashCode;

public class CatalogSchemaIdentifierName {
  private final CatalogName catalogName;
  private final SchemaName schemaName;
  private final String identifierName;

  private static final String DOT = ".";

  /**
   * Data model for capturing catalog, schema and identifier from
   * fully qualifiedName. In the current state, it is used to capture
   * CatalogSchemaTable name and CatalogSchemaFunction in case of table
   * functions.
   *
   * @param parts parts of qualifiedName.
   * @param allowedCatalogs allowedCatalogs.
   */
  public CatalogSchemaIdentifierName(List<String> parts, Set<String> allowedCatalogs) {
    catalogName = new CatalogName();
    schemaName = new SchemaName();
    List<String> remainingParts = schemaName.capture(catalogName.capture(parts, allowedCatalogs));
    identifierName = String.join(DOT, remainingParts);
  }

  public String getIdentifierName() {
    return identifierName;
  }

  public String getCatalogName() {
    return catalogName.getName();
  }

  public String getSchemaName() {
    return schemaName.getName();
  }

}

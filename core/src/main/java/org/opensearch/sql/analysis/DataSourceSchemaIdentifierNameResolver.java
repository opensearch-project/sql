/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.analysis;

import java.util.List;
import java.util.Set;

public class DataSourceSchemaIdentifierNameResolver {

  public static final String DEFAULT_DATASOURCE_NAME = "@opensearch";
  public static final String DEFAULT_SCHEMA_NAME = "default";
  public static final String INFORMATION_SCHEMA_NAME = "information_schema";

  private String dataSourceName = DEFAULT_DATASOURCE_NAME;
  private String schemaName = DEFAULT_SCHEMA_NAME;
  private String identifierName;

  private static final String DOT = ".";

  /**
   * Data model for capturing dataSourceName, schema and identifier from
   * fully qualifiedName. In the current state, it is used to capture
   * DataSourceSchemaTable name and DataSourceSchemaFunction in case of table
   * functions.
   *
   * @param parts           parts of qualifiedName.
   * @param allowedDataSources allowedDataSources.
   */
  public DataSourceSchemaIdentifierNameResolver(List<String> parts,
                                                Set<String> allowedDataSources) {
    List<String> remainingParts
        = captureSchemaName(captureDataSourceName(parts, allowedDataSources));
    identifierName = String.join(DOT, remainingParts);
  }

  public String getIdentifierName() {
    return identifierName;
  }

  public String getDataSourceName() {
    return dataSourceName;
  }

  public String getSchemaName() {
    return schemaName;
  }


  // Capture datasource name and return remaining parts(schema name and table name)
  // from the fully qualified name.
  private List<String> captureDataSourceName(List<String> parts, Set<String> allowedDataSources) {
    if (parts.size() > 1 && allowedDataSources.contains(parts.get(0))
        || DEFAULT_DATASOURCE_NAME.equals(parts.get(0))) {
      dataSourceName = parts.get(0);
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

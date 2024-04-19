/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import java.util.Map;
import java.util.Set;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

/** DataSource Service manage {@link DataSource}. */
public interface DataSourceService {

  /**
   * Returns {@link DataSource} corresponding to the DataSource name only if the datasource is
   * active and authorized.
   *
   * @param dataSourceName Name of the {@link DataSource}.
   * @return {@link DataSource}.
   */
  DataSource getDataSource(String dataSourceName);

  /**
   * Returns all dataSource Metadata objects. The returned objects won't contain any of the
   * credential info.
   *
   * @param isDefaultDataSourceRequired is used to specify if default opensearch connector is
   *     required in the output list.
   * @return set of {@link DataSourceMetadata}.
   */
  Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired);

  /**
   * Returns dataSourceMetadata object with specific name. The returned objects won't contain any
   * crendetial info.
   *
   * @param name name of the {@link DataSource}.
   * @return set of {@link DataSourceMetadata}.
   */
  DataSourceMetadata getDataSourceMetadata(String name);

  /**
   * Register {@link DataSource} defined by {@link DataSourceMetadata}.
   *
   * @param metadata {@link DataSourceMetadata}.
   */
  void createDataSource(DataSourceMetadata metadata);

  /**
   * Updates {@link DataSource} corresponding to dataSourceMetadata (all fields needed).
   *
   * @param dataSourceMetadata {@link DataSourceMetadata}.
   */
  void updateDataSource(DataSourceMetadata dataSourceMetadata);

  /**
   * Patches {@link DataSource} corresponding to the given name (only fields to be changed needed).
   *
   * @param dataSourceData
   */
  void patchDataSource(Map<String, Object> dataSourceData);

  /**
   * Deletes {@link DataSource} corresponding to the DataSource name.
   *
   * @param dataSourceName name of the {@link DataSource}.
   */
  void deleteDataSource(String dataSourceName);

  /**
   * Returns true {@link Boolean} if datasource with dataSourceName exists or else false {@link
   * Boolean}.
   *
   * @param dataSourceName name of the {@link DataSource}.
   */
  Boolean dataSourceExists(String dataSourceName);

  /**
   * Performs authorization and datasource status check and then returns RawDataSourceMetadata.
   * Specifically for addressing use cases in SparkQueryDispatcher.
   *
   * @param dataSourceName of the {@link DataSource}
   */
  DataSourceMetadata verifyDataSourceAccessAndGetRawMetadata(String dataSourceName);
}

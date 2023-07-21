/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasources.auth;

import org.opensearch.sql.datasource.model.DataSourceMetadata;

/**
 * Interface for datasource authorization helper.
 * The implementation of this class helps in determining
 * if authorization is required and the roles associated with the user.
 */
public interface DataSourceUserAuthorizationHelper {

  /**
   * Authorizes DataSource within the current context.
   *
   * @param dataSourceMetadata {@link DataSourceMetadata}
   */
  void authorizeDataSource(DataSourceMetadata dataSourceMetadata);
}

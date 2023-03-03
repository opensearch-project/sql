/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasource;

import java.util.List;
import java.util.Optional;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

/**
 * Interface for DataSourceMetadata Storage.
 */
public interface DataSourceMetadataStorage {

  List<DataSourceMetadata> getDataSourceMetadata();

  Optional<DataSourceMetadata> getDataSourceMetadata(String datasourceName);

  void createDataSourceMetadata(DataSourceMetadata dataSourceMetadata);

}
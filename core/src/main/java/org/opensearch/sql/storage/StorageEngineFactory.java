/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.storage;

import java.util.Map;
import org.opensearch.sql.catalog.model.ConnectorType;

public interface StorageEngineFactory {

  ConnectorType getConnectorType();

  StorageEngine getStorageEngine(String catalogName, Map<String, String> requiredConfig);

}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.table;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Base class for table DDL.
 */
@RequiredArgsConstructor
public abstract class TableDataDefinitionTask implements DataDefinitionTask {

  protected final StorageEngine storageEngine;

}

/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

import java.util.Collection;
import java.util.Collections;


/**
 * Spark storage engine implementation.
 */
@RequiredArgsConstructor
public class SparkStorageEngine implements StorageEngine {

    private final SparkClient sparkClient;

    @Override
    public Collection<FunctionResolver> getFunctions() {
        //TODO: add SqlTableFunctionResolver to list
        return Collections.singletonList(null);
    }

    @Override
    public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
        return null;
    }
}
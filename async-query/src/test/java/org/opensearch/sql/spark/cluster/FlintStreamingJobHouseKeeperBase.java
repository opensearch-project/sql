/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceSpec;
import org.opensearch.sql.spark.asyncquery.model.MockFlintIndex;
import org.opensearch.sql.spark.flint.FlintIndexType;

import java.util.HashMap;

public class FlintStreamingJobHouseKeeperBase extends AsyncQueryExecutorServiceSpec {
    protected void changeDataSourceStatus(String dataSourceName, DataSourceStatus dataSourceStatus) {
        HashMap<String, Object> datasourceMap = new HashMap<>();
        datasourceMap.put("name", dataSourceName);
        datasourceMap.put("status", dataSourceStatus);
        this.dataSourceService.patchDataSource(datasourceMap);
    }


    protected ImmutableList<MockFlintIndex> getMockFlintIndices() {
        return ImmutableList.of(getSkipping(), getCovering(), getMv());
    }

    protected MockFlintIndex getMv() {
        return new MockFlintIndex(
                client,
                "flint_my_glue_mydb_mv",
                FlintIndexType.MATERIALIZED_VIEW,
                "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                        + " incremental_refresh=true, output_mode=\"complete\") ");
    }

    protected MockFlintIndex getCovering() {
        return new MockFlintIndex(
                client,
                "flint_my_glue_mydb_http_logs_covering_index",
                FlintIndexType.COVERING,
                "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                        + " incremental_refresh=true, output_mode=\"complete\")");
    }

    protected MockFlintIndex getSkipping() {
        return new MockFlintIndex(
                client,
                "flint_my_glue_mydb_http_logs_skipping_index",
                FlintIndexType.SKIPPING,
                "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                        + " incremental_refresh=true, output_mode=\"complete\")");
    }
}

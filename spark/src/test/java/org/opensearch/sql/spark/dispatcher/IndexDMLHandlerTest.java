/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.datasource.model.DataSourceStatus.ACTIVE;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import java.util.HashMap;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.flint.IndexDMLResultStorageService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
class IndexDMLHandlerTest {

  @Mock private JobExecutionResponseReader jobExecutionResponseReader;
  @Mock private FlintIndexMetadataService flintIndexMetadataService;
  @Mock private IndexDMLResultStorageService indexDMLResultStorageService;
  @Mock private FlintIndexOpFactory flintIndexOpFactory;

  @Test
  public void getResponseFromExecutor() {
    JSONObject result = new IndexDMLHandler(null, null, null, null).getResponseFromExecutor(null);

    assertEquals("running", result.getString(STATUS_FIELD));
    assertEquals("", result.getString(ERROR_FIELD));
  }

  @Test
  public void testWhenIndexDetailsAreNotFound() {
    IndexDMLHandler indexDMLHandler =
        new IndexDMLHandler(
            jobExecutionResponseReader,
            flintIndexMetadataService,
            indexDMLResultStorageService,
            flintIndexOpFactory);
    DispatchQueryRequest dispatchQueryRequest =
        new DispatchQueryRequest(
            EMRS_APPLICATION_ID,
            "DROP INDEX",
            "my_glue",
            LangType.SQL,
            EMRS_EXECUTION_ROLE,
            TEST_CLUSTER_NAME);
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName("mys3")
            .setDescription("test description")
            .setConnector(DataSourceType.S3GLUE)
            .setDataSourceStatus(ACTIVE)
            .build();
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .mvName("mys3.default.http_logs_metrics")
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build();
    DispatchQueryContext dispatchQueryContext =
        DispatchQueryContext.builder()
            .dataSourceMetadata(metadata)
            .indexQueryDetails(indexQueryDetails)
            .build();
    Mockito.when(flintIndexMetadataService.getFlintIndexMetadata(any()))
        .thenReturn(new HashMap<>());

    DispatchQueryResponse dispatchQueryResponse =
        indexDMLHandler.submit(dispatchQueryRequest, dispatchQueryContext);

    Assertions.assertNotNull(dispatchQueryResponse.getQueryId());
  }

  @Test
  public void testWhenIndexDetailsWithInvalidQueryActionType() {
    FlintIndexMetadata flintIndexMetadata = mock(FlintIndexMetadata.class);
    IndexDMLHandler indexDMLHandler =
        new IndexDMLHandler(
            jobExecutionResponseReader,
            flintIndexMetadataService,
            indexDMLResultStorageService,
            flintIndexOpFactory);
    DispatchQueryRequest dispatchQueryRequest =
        new DispatchQueryRequest(
            EMRS_APPLICATION_ID,
            "CREATE INDEX",
            "my_glue",
            LangType.SQL,
            EMRS_EXECUTION_ROLE,
            TEST_CLUSTER_NAME);
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName("mys3")
            .setDescription("test description")
            .setConnector(DataSourceType.S3GLUE)
            .setDataSourceStatus(ACTIVE)
            .build();
    IndexQueryDetails indexQueryDetails =
        IndexQueryDetails.builder()
            .mvName("mys3.default.http_logs_metrics")
            .indexQueryActionType(IndexQueryActionType.CREATE)
            .indexType(FlintIndexType.MATERIALIZED_VIEW)
            .build();
    DispatchQueryContext dispatchQueryContext =
        DispatchQueryContext.builder()
            .dataSourceMetadata(metadata)
            .indexQueryDetails(indexQueryDetails)
            .build();
    HashMap<String, FlintIndexMetadata> flintMetadataMap = new HashMap<>();
    flintMetadataMap.put(indexQueryDetails.openSearchIndexName(), flintIndexMetadata);
    when(flintIndexMetadataService.getFlintIndexMetadata(indexQueryDetails.openSearchIndexName()))
        .thenReturn(flintMetadataMap);

    indexDMLHandler.submit(dispatchQueryRequest, dispatchQueryContext);
  }

  @Test
  public void testStaticMethods() {
    Assertions.assertTrue(IndexDMLHandler.isIndexDMLQuery("dropIndexJobId"));
  }
}

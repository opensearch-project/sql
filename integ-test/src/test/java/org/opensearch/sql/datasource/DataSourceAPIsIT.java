package org.opensearch.sql.datasource;/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class DataSourceAPIsIT extends PPLIntegTestCase {

  @Test
  public void createDataSourceTest() throws IOException {
    Request request = getCreateDataSourceRequest(getDataSourceMetadataJsonString());
    String response = executeRequest(request);
    Assert.assertEquals("Created DataSource with name prometheus1", response);
  }

  private Request getCreateDataSourceRequest(String dataSourceMetadataJson) {
    Request request = new Request("POST", "/_plugins/_query/_datasources");
    request.setJsonEntity(dataSourceMetadataJson);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  private String getDataSourceMetadataJsonString() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("prometheus1");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(new ArrayList<>());
    Map<String, String> propertiesMap = new HashMap<>();
    propertiesMap.put("prometheus.uri", "http://localhost:9200");
    dataSourceMetadata.setProperties(propertiesMap);
    return new Gson().toJson(dataSourceMetadata);
  }

}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.datasource.model.DataSourceType;

class SerializeUtilsTest {
  @Test
  public void buildGson_serializeDataSourceTypeAsString() {
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata.Builder()
            .setName("DATASOURCE_NAME")
            .setDescription("DESCRIPTION")
            .setConnector(DataSourceType.S3GLUE)
            .setAllowedRoles(ImmutableList.of("ROLE"))
            .setResultIndex("query_execution_result_test")
            .setDataSourceStatus(DataSourceStatus.ACTIVE)
            .build();

    Gson gson = SerializeUtils.buildGson();
    String json = gson.toJson(dataSourceMetadata);

    // connector should be serialized as string (not as object)
    assertJsonAttribute(json, "connector", "S3GLUE");
    // other attribute should be serialized as normal
    assertJsonAttribute(json, "name", "DATASOURCE_NAME");
    assertJsonAttribute(json, "description", "DESCRIPTION");
    assertJsonAttribute(json, "resultIndex", "query_execution_result_test");
    assertJsonAttribute(json, "status", "ACTIVE");
    assertTrue(json.contains("\"allowedRoles\":[\"ROLE\"]"));
  }

  private void assertJsonAttribute(String json, String attribute, String value) {
    assertTrue(json.contains("\"" + attribute + "\":\"" + value + "\""));
  }

  @Test
  public void buildGson_deserializeDataSourceTypeFromString() {
    String json =
        "{\"name\":\"DATASOURCE_NAME\","
            + "\"description\":\"DESCRIPTION\","
            + "\"connector\":\"S3GLUE\","
            + "\"allowedRoles\":[\"ROLE\"],"
            + "\"properties\":{},"
            + "\"resultIndex\":\"query_execution_result_test\","
            + "\"status\":\"ACTIVE\""
            + "}";

    Gson gson = SerializeUtils.buildGson();
    DataSourceMetadata metadata = gson.fromJson(json, DataSourceMetadata.class);

    assertEquals(DataSourceType.S3GLUE, metadata.getConnector());
  }
}

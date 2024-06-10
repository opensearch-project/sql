package org.opensearch.sql.datasources.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.datasource.model.DataSourceStatus.ACTIVE;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.*;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.utils.SerializeUtils;

@ExtendWith(MockitoExtension.class)
public class XContentParserUtilsTest {

  @SneakyThrows
  @Test
  public void testConvertToXContent() {
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata.Builder()
            .setName("testDS")
            .setConnector(DataSourceType.PROMETHEUS)
            .setAllowedRoles(List.of("prometheus_access"))
            .setProperties(Map.of("prometheus.uri", "https://localhost:9090"))
            .build();

    XContentBuilder contentBuilder = XContentParserUtils.convertToXContent(dataSourceMetadata);
    String contentString = BytesReference.bytes(contentBuilder).utf8ToString();
    Assertions.assertEquals(
        "{\"name\":\"testDS\",\"description\":\"\",\"connector\":\"PROMETHEUS\",\"allowedRoles\":[\"prometheus_access\"],\"properties\":{\"prometheus.uri\":\"https://localhost:9090\"},\"resultIndex\":\"query_execution_result_testds\",\"status\":\"ACTIVE\"}",
        contentString);
  }

  @SneakyThrows
  @Test
  public void testToDataSourceMetadataFromJson() {
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata.Builder()
            .setName("testDS")
            .setConnector(DataSourceType.PROMETHEUS)
            .setAllowedRoles(List.of("prometheus_access"))
            .setProperties(Map.of("prometheus.uri", "https://localhost:9090"))
            .setResultIndex("query_execution_result2")
            .build();
    Gson gson = SerializeUtils.buildGson();
    String json = gson.toJson(dataSourceMetadata);

    DataSourceMetadata retrievedMetadata = XContentParserUtils.toDataSourceMetadata(json);

    Assertions.assertEquals(retrievedMetadata, dataSourceMetadata);
    Assertions.assertEquals("prometheus_access", retrievedMetadata.getAllowedRoles().get(0));
  }

  @SneakyThrows
  @Test
  public void testToMapFromJson() {
    Map<String, Object> dataSourceData =
        Map.of(
            NAME_FIELD,
            "test_DS",
            DESCRIPTION_FIELD,
            "test",
            ALLOWED_ROLES_FIELD,
            List.of("all_access"),
            PROPERTIES_FIELD,
            Map.of("prometheus.uri", "localhost:9090"),
            CONNECTOR_FIELD,
            "PROMETHEUS",
            RESULT_INDEX_FIELD,
            "",
            STATUS_FIELD,
            ACTIVE);

    Map<String, Object> dataSourceDataConnectorRemoved =
        Map.of(
            NAME_FIELD,
            "test_DS",
            DESCRIPTION_FIELD,
            "test",
            ALLOWED_ROLES_FIELD,
            List.of("all_access"),
            PROPERTIES_FIELD,
            Map.of("prometheus.uri", "localhost:9090"),
            RESULT_INDEX_FIELD,
            "",
            STATUS_FIELD,
            ACTIVE);

    String json = SerializeUtils.buildGson().toJson(dataSourceData);

    Map<String, Object> parsedData = XContentParserUtils.toMap(json);

    Assertions.assertEquals(parsedData, dataSourceDataConnectorRemoved);
    Assertions.assertEquals("test", parsedData.get(DESCRIPTION_FIELD));
  }

  @SneakyThrows
  @Test
  public void testToDataSourceMetadataFromJsonWithoutNameAndConnector() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              XContentParserUtils.toDataSourceMetadata(
                  "{\"description\":\"\",\"allowedRoles\":[\"prometheus_access\"],\"resultIndex\":\"query_execution_result_testds\",\"status\":\"ACTIVE\"}");
            });
    Assertions.assertEquals(
        "Datasource configuration error: name, connector cannot be null or empty.",
        exception.getMessage());
  }

  @SneakyThrows
  @Test
  public void testToMapFromJsonWithoutName() {
    Map<String, Object> dataSourceData = new HashMap<>(Map.of(DESCRIPTION_FIELD, "test"));
    String json = SerializeUtils.buildGson().toJson(dataSourceData);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              XContentParserUtils.toMap(json);
            });
    Assertions.assertEquals("Name is a required field.", exception.getMessage());
  }

  @SneakyThrows
  @Test
  public void testToDataSourceMetadataFromJsonUsingUnknownObject() {
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put("test", "test");
    String json = SerializeUtils.buildGson().toJson(hashMap);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              XContentParserUtils.toDataSourceMetadata(json);
            });
    Assertions.assertEquals("Unknown field: test", exception.getMessage());
  }

  @SneakyThrows
  @Test
  public void testToMapFromJsonUsingUnknownObject() {
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put("test", "test");
    String json = SerializeUtils.buildGson().toJson(hashMap);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              XContentParserUtils.toMap(json);
            });
    Assertions.assertEquals("Unknown field: test", exception.getMessage());
  }
}

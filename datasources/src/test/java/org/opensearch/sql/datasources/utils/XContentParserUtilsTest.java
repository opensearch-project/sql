package org.opensearch.sql.datasources.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;

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

@ExtendWith(MockitoExtension.class)
public class XContentParserUtilsTest {

  @SneakyThrows
  @Test
  public void testConvertToXContent() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(List.of("prometheus_access"));
    dataSourceMetadata.setProperties(Map.of("prometheus.uri", "https://localhost:9090"));

    XContentBuilder contentBuilder = XContentParserUtils.convertToXContent(dataSourceMetadata);
    String contentString = BytesReference.bytes(contentBuilder).utf8ToString();
    Assertions.assertEquals(
        "{\"name\":\"testDS\",\"description\":\"\",\"connector\":\"PROMETHEUS\",\"allowedRoles\":[\"prometheus_access\"],\"properties\":{\"prometheus.uri\":\"https://localhost:9090\"},\"resultIndex\":null}",
        contentString);
  }

  @SneakyThrows
  @Test
  public void testToDataSourceMetadataFromJson() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("testDS");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(List.of("prometheus_access"));
    dataSourceMetadata.setProperties(Map.of("prometheus.uri", "https://localhost:9090"));
    Gson gson = new Gson();
    String json = gson.toJson(dataSourceMetadata);

    DataSourceMetadata retrievedMetadata = XContentParserUtils.toDataSourceMetadata(json);

    Assertions.assertEquals(retrievedMetadata, dataSourceMetadata);
    Assertions.assertEquals("prometheus_access", retrievedMetadata.getAllowedRoles().get(0));
  }

  @SneakyThrows
  @Test
  public void testToDataSourceMetadataFromJsonWithoutName() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    dataSourceMetadata.setAllowedRoles(List.of("prometheus_access"));
    dataSourceMetadata.setProperties(Map.of("prometheus.uri", "https://localhost:9090"));
    Gson gson = new Gson();
    String json = gson.toJson(dataSourceMetadata);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              XContentParserUtils.toDataSourceMetadata(json);
            });
    Assertions.assertEquals("name and connector are required fields.", exception.getMessage());
  }

  @SneakyThrows
  @Test
  public void testToDataSourceMetadataFromJsonWithoutConnector() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("name");
    dataSourceMetadata.setAllowedRoles(List.of("prometheus_access"));
    dataSourceMetadata.setProperties(Map.of("prometheus.uri", "https://localhost:9090"));
    Gson gson = new Gson();
    String json = gson.toJson(dataSourceMetadata);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              XContentParserUtils.toDataSourceMetadata(json);
            });
    Assertions.assertEquals("name and connector are required fields.", exception.getMessage());
  }

  @SneakyThrows
  @Test
  public void testToDataSourceMetadataFromJsonUsingUnknownObject() {
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put("test", "test");
    Gson gson = new Gson();
    String json = gson.toJson(hashMap);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              XContentParserUtils.toDataSourceMetadata(json);
            });
    Assertions.assertEquals("Unknown field: test", exception.getMessage());
  }
}

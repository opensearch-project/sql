package org.opensearch.sql.datasources.glue;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

@ExtendWith(MockitoExtension.class)
public class GlueDataSourceFactoryTest {

  @Mock private Settings settings;

  @Test
  void testGetConnectorType() {
    GlueDataSourceFactory glueDatasourceFactory = new GlueDataSourceFactory(settings);
    Assertions.assertEquals(DataSourceType.S3GLUE, glueDatasourceFactory.getDataSourceType());
  }

  @Test
  @SneakyThrows
  void testCreateGLueDatSource() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    GlueDataSourceFactory glueDatasourceFactory = new GlueDataSourceFactory(settings);

    DataSourceMetadata metadata = new DataSourceMetadata();
    HashMap<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "noauth");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");

    metadata.setName("my_glue");
    metadata.setConnector(DataSourceType.S3GLUE);
    metadata.setProperties(properties);
    DataSource dataSource = glueDatasourceFactory.createDataSource(metadata);
    Assertions.assertEquals(DataSourceType.S3GLUE, dataSource.getConnectorType());
    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                dataSource
                    .getStorageEngine()
                    .getTable(new DataSourceSchemaName("my_glue", "default"), "alb_logs"));
    Assertions.assertEquals(
        "Glue storage engine is not supported.", unsupportedOperationException.getMessage());
  }

  @Test
  @SneakyThrows
  void testCreateGLueDatSourceWithBasicAuthForIndexStore() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    GlueDataSourceFactory glueDatasourceFactory = new GlueDataSourceFactory(settings);

    DataSourceMetadata metadata = new DataSourceMetadata();
    HashMap<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "basicauth");
    properties.put("glue.indexstore.opensearch.auth.username", "username");
    properties.put("glue.indexstore.opensearch.auth.password", "password");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");

    metadata.setName("my_glue");
    metadata.setConnector(DataSourceType.S3GLUE);
    metadata.setProperties(properties);
    DataSource dataSource = glueDatasourceFactory.createDataSource(metadata);
    Assertions.assertEquals(DataSourceType.S3GLUE, dataSource.getConnectorType());
    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                dataSource
                    .getStorageEngine()
                    .getTable(new DataSourceSchemaName("my_glue", "default"), "alb_logs"));
    Assertions.assertEquals(
        "Glue storage engine is not supported.", unsupportedOperationException.getMessage());
  }

  @Test
  @SneakyThrows
  void testCreateGLueDatSourceWithAwsSigV4AuthForIndexStore() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    GlueDataSourceFactory glueDatasourceFactory = new GlueDataSourceFactory(settings);

    DataSourceMetadata metadata = new DataSourceMetadata();
    HashMap<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "awssigv4");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");

    metadata.setName("my_glue");
    metadata.setConnector(DataSourceType.S3GLUE);
    metadata.setProperties(properties);
    DataSource dataSource = glueDatasourceFactory.createDataSource(metadata);
    Assertions.assertEquals(DataSourceType.S3GLUE, dataSource.getConnectorType());
    UnsupportedOperationException unsupportedOperationException =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                dataSource
                    .getStorageEngine()
                    .getTable(new DataSourceSchemaName("my_glue", "default"), "alb_logs"));
    Assertions.assertEquals(
        "Glue storage engine is not supported.", unsupportedOperationException.getMessage());
  }

  @Test
  @SneakyThrows
  void testCreateGLueDatSourceWithBasicAuthForIndexStoreAndMissingFields() {
    GlueDataSourceFactory glueDatasourceFactory = new GlueDataSourceFactory(settings);

    DataSourceMetadata metadata = new DataSourceMetadata();
    HashMap<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "basicauth");

    metadata.setName("my_glue");
    metadata.setConnector(DataSourceType.S3GLUE);
    metadata.setProperties(properties);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> glueDatasourceFactory.createDataSource(metadata));
    Assertions.assertEquals(
        "Missing [glue.indexstore.opensearch.auth.password,"
            + " glue.indexstore.opensearch.auth.username] fields in the connector properties.",
        illegalArgumentException.getMessage());
  }

  @Test
  @SneakyThrows
  void testCreateGLueDatSourceWithInvalidFlintHost() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(List.of("127.0.0.0/8"));
    GlueDataSourceFactory glueDatasourceFactory = new GlueDataSourceFactory(settings);

    DataSourceMetadata metadata = new DataSourceMetadata();
    HashMap<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "noauth");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");

    metadata.setName("my_glue");
    metadata.setConnector(DataSourceType.S3GLUE);
    metadata.setProperties(properties);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> glueDatasourceFactory.createDataSource(metadata));
    Assertions.assertEquals(
        "Disallowed hostname in the uri. "
            + "Validate with plugins.query.datasources.uri.hosts.denylist config",
        illegalArgumentException.getMessage());
  }

  @Test
  @SneakyThrows
  void testCreateGLueDatSourceWithInvalidFlintHostSyntax() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(List.of("127.0.0.0/8"));
    GlueDataSourceFactory glueDatasourceFactory = new GlueDataSourceFactory(settings);

    DataSourceMetadata metadata = new DataSourceMetadata();
    HashMap<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put(
        "glue.indexstore.opensearch.uri",
        "http://dummyprometheus.com:9090? paramt::localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "noauth");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");

    metadata.setName("my_glue");
    metadata.setConnector(DataSourceType.S3GLUE);
    metadata.setProperties(properties);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> glueDatasourceFactory.createDataSource(metadata));
    Assertions.assertEquals(
        "Invalid flint host in properties.", illegalArgumentException.getMessage());
  }
}

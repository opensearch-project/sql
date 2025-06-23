package org.opensearch.sql.datasources.glue;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

@ExtendWith(MockitoExtension.class)
public class SecurityLakeSourceFactoryTest {

  @Mock private Settings settings;

  @Test
  void testGetConnectorType() {
    SecurityLakeDataSourceFactory securityLakeDataSourceFactory =
        new SecurityLakeDataSourceFactory(settings);
    Assertions.assertEquals(
        DataSourceType.SECURITY_LAKE, securityLakeDataSourceFactory.getDataSourceType());
  }

  @Test
  @SneakyThrows
  void testCreateSecurityLakeDataSource() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    SecurityLakeDataSourceFactory securityLakeDataSourceFactory =
        new SecurityLakeDataSourceFactory(settings);

    Map<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "noauth");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");
    properties.put("glue.lakeformation.session_tag", "session_tag");
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName("my_sl")
            .setConnector(DataSourceType.SECURITY_LAKE)
            .setProperties(properties)
            .build();
    DataSource dataSource = securityLakeDataSourceFactory.createDataSource(metadata);
    Assertions.assertEquals(DataSourceType.SECURITY_LAKE, dataSource.getConnectorType());

    Assertions.assertEquals(
        properties.get(GlueDataSourceFactory.GLUE_ICEBERG_ENABLED),
        SecurityLakeDataSourceFactory.TRUE);
    Assertions.assertEquals(
        properties.get(GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED),
        SecurityLakeDataSourceFactory.TRUE);
  }

  @Test
  @SneakyThrows
  void testCreateSecurityLakeDataSourceIcebergCannotBeDisabled() {
    SecurityLakeDataSourceFactory securityLakeDataSourceFactory =
        new SecurityLakeDataSourceFactory(settings);

    Map<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "noauth");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");
    properties.put("glue.iceberg.enabled", "false");
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName("my_sl")
            .setConnector(DataSourceType.SECURITY_LAKE)
            .setProperties(properties)
            .build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> securityLakeDataSourceFactory.createDataSource(metadata));
  }

  @Test
  @SneakyThrows
  void testCreateSecurityLakeDataSourceLakeFormationCannotBeDisabled() {
    SecurityLakeDataSourceFactory securityLakeDataSourceFactory =
        new SecurityLakeDataSourceFactory(settings);

    Map<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "noauth");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");
    properties.put("glue.iceberg.enabled", "true");
    properties.put("glue.lakeformation.enabled", "false");
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName("my_sl")
            .setConnector(DataSourceType.SECURITY_LAKE)
            .setProperties(properties)
            .build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> securityLakeDataSourceFactory.createDataSource(metadata));
  }

  @Test
  @SneakyThrows
  void testCreateGlueDataSourceWithLakeFormationNoSessionTags() {
    SecurityLakeDataSourceFactory securityLakeDataSourceFactory =
        new SecurityLakeDataSourceFactory(settings);

    HashMap<String, String> properties = new HashMap<>();
    properties.put("glue.auth.type", "iam_role");
    properties.put("glue.auth.role_arn", "role_arn");
    properties.put("glue.indexstore.opensearch.uri", "http://localhost:9200");
    properties.put("glue.indexstore.opensearch.auth", "noauth");
    properties.put("glue.indexstore.opensearch.region", "us-west-2");
    properties.put("glue.iceberg.enabled", "true");
    properties.put("glue.lakeformation.enabled", "true");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName("my_sl")
            .setConnector(DataSourceType.SECURITY_LAKE)
            .setProperties(properties)
            .build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> securityLakeDataSourceFactory.createDataSource(metadata));
  }
}

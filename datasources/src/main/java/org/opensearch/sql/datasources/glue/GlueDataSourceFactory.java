package org.opensearch.sql.datasources.glue;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.datasources.utils.DatasourceValidationUtils;
import org.opensearch.sql.storage.DataSourceFactory;

@RequiredArgsConstructor
public class GlueDataSourceFactory implements DataSourceFactory {

  private final Settings pluginSettings;

  // Glue configuration properties
  public static final String GLUE_AUTH_TYPE = "glue.auth.type";
  public static final String GLUE_ROLE_ARN = "glue.auth.role_arn";
  public static final String GLUE_INDEX_STORE_OPENSEARCH_URI = "glue.indexstore.opensearch.uri";
  public static final String GLUE_INDEX_STORE_OPENSEARCH_AUTH = "glue.indexstore.opensearch.auth";
  public static final String GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME =
      "glue.indexstore.opensearch.auth.username";
  public static final String GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD =
      "glue.indexstore.opensearch.auth.password";
  public static final String GLUE_INDEX_STORE_OPENSEARCH_REGION =
      "glue.indexstore.opensearch.region";
  public static final String GLUE_ASYNC_QUERY_ENABLED =
      "glue.async_query.enabled";

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.S3GLUE;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    try {
      validateGlueDataSourceConfiguration(metadata.getProperties());
      return new DataSource(
          metadata.getName(),
          metadata.getConnector(),
          (dataSourceSchemaName, tableName) -> {
            throw new UnsupportedOperationException("Glue storage engine is not supported.");
          });
    } catch (URISyntaxException | UnknownHostException e) {
      throw new IllegalArgumentException("Invalid flint host in properties.");
    }
  }

  private void validateGlueDataSourceConfiguration(Map<String, String> dataSourceMetadataConfig)
      throws URISyntaxException, UnknownHostException {

    DatasourceValidationUtils.validateLengthAndRequiredFields(
        dataSourceMetadataConfig,
        Set.of(
            GLUE_AUTH_TYPE,
            GLUE_ROLE_ARN,
            GLUE_INDEX_STORE_OPENSEARCH_URI,
            GLUE_INDEX_STORE_OPENSEARCH_AUTH));
    AuthenticationType authenticationType =
        AuthenticationType.get(dataSourceMetadataConfig.get(GLUE_INDEX_STORE_OPENSEARCH_AUTH));
    if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
      DatasourceValidationUtils.validateLengthAndRequiredFields(
          dataSourceMetadataConfig,
          Set.of(
              GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME,
              GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD));
    } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
      DatasourceValidationUtils.validateLengthAndRequiredFields(
          dataSourceMetadataConfig, Set.of(GLUE_INDEX_STORE_OPENSEARCH_REGION));
    }
    DatasourceValidationUtils.validateHost(
        dataSourceMetadataConfig.get(GLUE_INDEX_STORE_OPENSEARCH_URI),
        pluginSettings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST));
  }
}

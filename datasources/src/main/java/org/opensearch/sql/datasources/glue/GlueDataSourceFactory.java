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
import org.opensearch.sql.datasources.utils.DatasourceValidationUtils;
import org.opensearch.sql.storage.DataSourceFactory;

@RequiredArgsConstructor
public class GlueDataSourceFactory implements DataSourceFactory {

  private final Settings pluginSettings;

  // Glue configuration properties
  public static final String GLUE_AUTH_TYPE = "glue.auth.type";
  public static final String GLUE_ROLE_ARN = "glue.auth.role_arn";
  public static final String FLINT_URI = "glue.indexstore.opensearch.uri";
  public static final String FLINT_AUTH = "glue.indexstore.opensearch.auth";
  public static final String FLINT_REGION = "glue.indexstore.opensearch.region";

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
        Set.of(GLUE_AUTH_TYPE, GLUE_ROLE_ARN, FLINT_URI, FLINT_REGION, FLINT_AUTH));
    DatasourceValidationUtils.validateHost(
        dataSourceMetadataConfig.get(FLINT_URI),
        pluginSettings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST));
  }
}

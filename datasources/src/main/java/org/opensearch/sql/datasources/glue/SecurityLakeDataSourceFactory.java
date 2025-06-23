package org.opensearch.sql.datasources.glue;

import java.util.Map;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

public class SecurityLakeDataSourceFactory extends GlueDataSourceFactory {

  private final Settings pluginSettings;

  public static final String TRUE = "true";

  public SecurityLakeDataSourceFactory(final Settings pluginSettings) {
    super(pluginSettings);
    this.pluginSettings = pluginSettings;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.SECURITY_LAKE;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    validateProperties(metadata.getProperties());
    metadata.getProperties().put(GlueDataSourceFactory.GLUE_ICEBERG_ENABLED, TRUE);
    metadata.getProperties().put(GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED, TRUE);
    return super.createDataSource(metadata);
  }

  private void validateProperties(Map<String, String> properties) {
    // validate Lake Formation config
    if (properties.get(GlueDataSourceFactory.GLUE_ICEBERG_ENABLED) != null
        && !BooleanUtils.toBoolean(properties.get(GlueDataSourceFactory.GLUE_ICEBERG_ENABLED))) {
      throw new IllegalArgumentException(
          GlueDataSourceFactory.GLUE_ICEBERG_ENABLED
              + " cannot be false when using Security Lake data source.");
    }

    if (properties.get(GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED) != null
        && !BooleanUtils.toBoolean(
            properties.get(GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED))) {
      throw new IllegalArgumentException(
          GLUE_LAKEFORMATION_ENABLED + " cannot be false when using Security Lake data source.");
    }

    if (StringUtils.isBlank(properties.get(GLUE_LAKEFORMATION_SESSION_TAG))) {
      throw new IllegalArgumentException(
          GlueDataSourceFactory.GLUE_LAKEFORMATION_SESSION_TAG
              + " must be specified when using Security Lake data source");
    }
  }
}

package org.opensearch.sql.api;

import java.util.Map;
import java.util.Set;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.RequestContext;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

/** A DataSourceService that assumes no access to data sources */
public class EmptyDataSourceService implements DataSourceService {
  public EmptyDataSourceService() {}

  @Override
  public DataSource getDataSource(String dataSourceName) {
    return null;
  }

  @Override
  public Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired) {
    return Set.of();
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String name) {
    return null;
  }

  @Override
  public void createDataSource(DataSourceMetadata metadata) {}

  @Override
  public void updateDataSource(DataSourceMetadata dataSourceMetadata) {}

  @Override
  public void patchDataSource(Map<String, Object> dataSourceData) {}

  @Override
  public void deleteDataSource(String dataSourceName) {}

  @Override
  public Boolean dataSourceExists(String dataSourceName) {
    return false;
  }

  @Override
  public DataSourceMetadata verifyDataSourceAccessAndGetRawMetadata(
      String dataSourceName, RequestContext context) {
    return null;
  }
}

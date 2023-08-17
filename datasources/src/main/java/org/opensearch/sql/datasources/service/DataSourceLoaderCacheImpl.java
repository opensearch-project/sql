package org.opensearch.sql.datasources.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;

/**
 * Default implementation of DataSourceLoaderCache. This implementation utilizes Google Guava Cache
 * {@link Cache} for caching DataSource objects against {@link DataSourceMetadata}. Expires the
 * cache objects every 24 hrs after the last access.
 */
public class DataSourceLoaderCacheImpl implements DataSourceLoaderCache {
  private final Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap;
  private final Cache<DataSourceMetadata, DataSource> dataSourceCache;

  /**
   * DataSourceLoaderCacheImpl constructor.
   *
   * @param dataSourceFactorySet set of {@link DataSourceFactory}.
   */
  public DataSourceLoaderCacheImpl(Set<DataSourceFactory> dataSourceFactorySet) {
    this.dataSourceFactoryMap =
        dataSourceFactorySet.stream()
            .collect(Collectors.toMap(DataSourceFactory::getDataSourceType, f -> f));
    this.dataSourceCache =
        CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(24, TimeUnit.HOURS).build();
  }

  @Override
  public DataSource getOrLoadDataSource(DataSourceMetadata dataSourceMetadata) {
    DataSource dataSource = this.dataSourceCache.getIfPresent(dataSourceMetadata);
    if (dataSource == null) {
      dataSource =
          this.dataSourceFactoryMap
              .get(dataSourceMetadata.getConnector())
              .createDataSource(dataSourceMetadata);
      this.dataSourceCache.put(dataSourceMetadata, dataSource);
      return dataSource;
    }
    return dataSource;
  }
}

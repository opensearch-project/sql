/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.service;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.*;

import java.util.*;
import java.util.stream.Collectors;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelper;
import org.opensearch.sql.datasources.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.datasources.exceptions.DatasourceDisabledException;
import org.opensearch.sql.storage.DataSourceFactory;

/**
 * Default implementation of {@link DataSourceService}. It is per-jvm single instance.
 *
 * <p>{@link DataSourceService} is constructed by the list of {@link DataSourceFactory} at service
 * bootstrap time. The set of {@link DataSourceFactory} is immutable. Client could add {@link
 * DataSource} defined by {@link DataSourceMetadata} at any time. {@link DataSourceService} use
 * {@link DataSourceFactory} to create {@link DataSource}.
 */
public class DataSourceServiceImpl implements DataSourceService {

  public static final Set<String> CONFIDENTIAL_AUTH_KEYS =
      Set.of("auth.username", "auth.password", "auth.access_key", "auth.secret_key");

  private final DataSourceLoaderCache dataSourceLoaderCache;

  private final DataSourceMetadataStorage dataSourceMetadataStorage;

  private final DataSourceUserAuthorizationHelper dataSourceUserAuthorizationHelper;

  /** Construct from the set of {@link DataSourceFactory} at bootstrap time. */
  public DataSourceServiceImpl(
      Set<DataSourceFactory> dataSourceFactories,
      DataSourceMetadataStorage dataSourceMetadataStorage,
      DataSourceUserAuthorizationHelper dataSourceUserAuthorizationHelper) {
    this.dataSourceMetadataStorage = dataSourceMetadataStorage;
    this.dataSourceUserAuthorizationHelper = dataSourceUserAuthorizationHelper;
    this.dataSourceLoaderCache = new DataSourceLoaderCacheImpl(dataSourceFactories);
  }

  @Override
  public Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired) {
    List<DataSourceMetadata> dataSourceMetadataList =
        this.dataSourceMetadataStorage.getDataSourceMetadata();
    Set<DataSourceMetadata> dataSourceMetadataSet = new HashSet<>(dataSourceMetadataList);
    if (isDefaultDataSourceRequired) {
      dataSourceMetadataSet.add(DataSourceMetadata.defaultOpenSearchDataSourceMetadata());
    }
    return removeAuthInfo(dataSourceMetadataSet);
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String dataSourceName) {
    DataSourceMetadata dataSourceMetadata = getRawDataSourceMetadata(dataSourceName);
    return removeAuthInfo(dataSourceMetadata);
  }

  @Override
  public DataSource getDataSource(String dataSourceName) {
    DataSourceMetadata dataSourceMetadata = getRawDataSourceMetadata(dataSourceName);
    verifyDataSourceAccess(dataSourceMetadata);
    return dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
  }

  @Override
  public void createDataSource(DataSourceMetadata metadata) {
    if (!metadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      this.dataSourceLoaderCache.getOrLoadDataSource(metadata);
      this.dataSourceMetadataStorage.createDataSourceMetadata(metadata);
    }
  }

  @Override
  public void updateDataSource(DataSourceMetadata dataSourceMetadata) {
    if (!dataSourceMetadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      this.dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
      this.dataSourceMetadataStorage.updateDataSourceMetadata(dataSourceMetadata);
    } else {
      throw new UnsupportedOperationException(
          "Not allowed to update default datasource :" + DEFAULT_DATASOURCE_NAME);
    }
  }

  @Override
  public void patchDataSource(Map<String, Object> dataSourceData) {
    if (!dataSourceData.get(NAME_FIELD).equals(DEFAULT_DATASOURCE_NAME)) {
      DataSourceMetadata dataSourceMetadata =
          getRawDataSourceMetadata((String) dataSourceData.get(NAME_FIELD));
      DataSourceMetadata updatedMetadata =
          constructUpdatedDatasourceMetadata(dataSourceData, dataSourceMetadata);
      updateDataSource(updatedMetadata);
    } else {
      throw new UnsupportedOperationException(
          "Not allowed to update default datasource :" + DEFAULT_DATASOURCE_NAME);
    }
  }

  @Override
  public void deleteDataSource(String dataSourceName) {
    if (dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
      throw new UnsupportedOperationException(
          "Not allowed to delete default datasource :" + DEFAULT_DATASOURCE_NAME);
    } else {
      this.dataSourceMetadataStorage.deleteDataSourceMetadata(dataSourceName);
    }
  }

  @Override
  public Boolean dataSourceExists(String dataSourceName) {
    return DEFAULT_DATASOURCE_NAME.equals(dataSourceName)
        || this.dataSourceMetadataStorage.getDataSourceMetadata(dataSourceName).isPresent();
  }

  @Override
  public DataSourceMetadata verifyDataSourceAccessAndGetRawMetadata(String dataSourceName) {
    DataSourceMetadata dataSourceMetadata = getRawDataSourceMetadata(dataSourceName);
    verifyDataSourceAccess(dataSourceMetadata);
    return dataSourceMetadata;
  }

  private void verifyDataSourceAccess(DataSourceMetadata dataSourceMetadata) {
    if (dataSourceMetadata.getStatus().equals(DataSourceStatus.DISABLED)) {
      throw new DatasourceDisabledException(
          String.format("Datasource %s is disabled.", dataSourceMetadata.getName()));
    }
    this.dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
  }

  /**
   * Replaces the fields in the map of the given metadata.
   *
   * @param dataSourceData
   * @param metadata {@link DataSourceMetadata}.
   */
  private DataSourceMetadata constructUpdatedDatasourceMetadata(
      Map<String, Object> dataSourceData, DataSourceMetadata metadata) {
    DataSourceMetadata.Builder metadataBuilder = new DataSourceMetadata.Builder(metadata);
    for (String key : dataSourceData.keySet()) {
      switch (key) {
          // Name and connector should not be modified
        case DESCRIPTION_FIELD:
          metadataBuilder.setDescription((String) dataSourceData.get(DESCRIPTION_FIELD));
          break;
        case ALLOWED_ROLES_FIELD:
          metadataBuilder.setAllowedRoles((List<String>) dataSourceData.get(ALLOWED_ROLES_FIELD));
          break;
        case PROPERTIES_FIELD:
          Map<String, String> properties = new HashMap<>(metadata.getProperties());
          properties.putAll(((Map<String, String>) dataSourceData.get(PROPERTIES_FIELD)));
          metadataBuilder.setProperties(properties);
          break;
        case RESULT_INDEX_FIELD:
          metadataBuilder.setResultIndex((String) dataSourceData.get(RESULT_INDEX_FIELD));
        case STATUS_FIELD:
          metadataBuilder.setDataSourceStatus((DataSourceStatus) dataSourceData.get(STATUS_FIELD));
        default:
          break;
      }
    }
    return metadataBuilder.build();
  }

  private DataSourceMetadata getRawDataSourceMetadata(String dataSourceName) {
    if (dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
      return DataSourceMetadata.defaultOpenSearchDataSourceMetadata();
    } else {
      Optional<DataSourceMetadata> dataSourceMetadataOptional =
          this.dataSourceMetadataStorage.getDataSourceMetadata(dataSourceName);
      if (dataSourceMetadataOptional.isEmpty()) {
        throw new DataSourceNotFoundException(
            String.format("DataSource with name %s doesn't exist.", dataSourceName));
      } else {
        return dataSourceMetadataOptional.get();
      }
    }
  }

  // It is advised to avoid sending any kind credential
  // info in api response from security point of view.
  private Set<DataSourceMetadata> removeAuthInfo(Set<DataSourceMetadata> dataSourceMetadataSet) {
    return dataSourceMetadataSet.stream().map(this::removeAuthInfo).collect(Collectors.toSet());
  }

  private DataSourceMetadata removeAuthInfo(DataSourceMetadata dataSourceMetadata) {
    HashMap<String, String> safeProperties = new HashMap<>(dataSourceMetadata.getProperties());
    safeProperties
        .entrySet()
        .removeIf(
            entry ->
                CONFIDENTIAL_AUTH_KEYS.stream()
                    .anyMatch(confidentialKey -> entry.getKey().endsWith(confidentialKey)));
    return new DataSourceMetadata.Builder(dataSourceMetadata).setProperties(safeProperties).build();
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.ConnectorType;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.prometheus.storage.PrometheusStorageFactory;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.StorageEngineFactory;

/**
 * This class manages datasources and responsible for creating connectors to the datasources.
 */
public class DataSourceServiceImpl implements DataSourceService {

  private static final DataSourceServiceImpl INSTANCE = new DataSourceServiceImpl();

  private static final String DATASOURCE_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";

  private static final Logger LOG = LogManager.getLogger();

  private Map<String, DataSource> datasourceMap = new HashMap<>();

  private final Map<ConnectorType, StorageEngineFactory> connectorTypeStorageEngineFactoryMap;

  public static DataSourceServiceImpl getInstance() {
    return INSTANCE;
  }

  private DataSourceServiceImpl() {
    connectorTypeStorageEngineFactoryMap = new HashMap<>();
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory();
    connectorTypeStorageEngineFactoryMap.put(prometheusStorageFactory.getConnectorType(),
        prometheusStorageFactory);
  }

  /**
   * This function reads settings and loads connectors to the data stores.
   * This will be invoked during start up and also when settings are updated.
   *
   * @param settings settings.
   */
  public void loadConnectors(Settings settings) {
    SecurityAccess.doPrivileged(() -> {
      InputStream inputStream = DataSourceSettings.DATASOURCE_CONFIG.get(settings);
      if (inputStream != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
          List<DataSourceMetadata> dataSourceMetadataList =
              objectMapper.readValue(inputStream, new TypeReference<>() {
              });
          validateDataSourceMetadata(dataSourceMetadataList);
          constructConnectors(dataSourceMetadataList);
        } catch (IOException e) {
          LOG.error("DataSources Configuration File uploaded is malformed. Verify and re-upload.",
              e);
        } catch (Throwable e) {
          LOG.error("DataSource construction failed.", e);
        }
      }
      return null;
    });
  }

  @Override
  public Set<DataSource> getDataSources() {
    return new HashSet<>(datasourceMap.values());
  }

  @Override
  public DataSource getDataSource(String dataSourceName) {
    if (!datasourceMap.containsKey(dataSourceName)) {
      throw new IllegalArgumentException(
          String.format("DataSource with name %s doesn't exist.", dataSourceName));
    }
    return datasourceMap.get(dataSourceName);
  }


  @Override
  public void registerDefaultOpenSearchDataSource(StorageEngine storageEngine) {
    if (storageEngine == null) {
      throw new IllegalArgumentException("Default storage engine can't be null");
    }
    datasourceMap.put(DEFAULT_DATASOURCE_NAME,
        new DataSource(DEFAULT_DATASOURCE_NAME, ConnectorType.OPENSEARCH, storageEngine));
    registerFunctions(DEFAULT_DATASOURCE_NAME, storageEngine);
  }

  private StorageEngine createStorageEngine(DataSourceMetadata dataSourceMetadata) {
    ConnectorType connector = dataSourceMetadata.getConnector();
    switch (connector) {
      case PROMETHEUS:
        StorageEngine storageEngine = connectorTypeStorageEngineFactoryMap
            .get(dataSourceMetadata.getConnector())
            .getStorageEngine(dataSourceMetadata.getName(), dataSourceMetadata.getProperties());
        registerFunctions(dataSourceMetadata.getName(), storageEngine);
        return storageEngine;
      default:
        throw new IllegalStateException(
            String.format("Unsupported Connector: %s", connector.name()));
    }
  }

  private void constructConnectors(List<DataSourceMetadata> dataSourceMetadataList) {
    datasourceMap = new HashMap<>();
    for (DataSourceMetadata dataSourceMetadata : dataSourceMetadataList) {
      try {
        String dataSourceName = dataSourceMetadata.getName();
        StorageEngine storageEngine = createStorageEngine(dataSourceMetadata);
        datasourceMap.put(dataSourceName,
            new DataSource(dataSourceMetadata.getName(), dataSourceMetadata.getConnector(),
                storageEngine));
      } catch (Throwable e) {
        LOG.error("DataSource : {} storage engine creation failed with the following message: {}",
            dataSourceMetadata.getName(), e.getMessage(), e);
      }
    }
  }

  /**
   * This can be moved to a different validator class
   * when we introduce more connectors.
   *
   * @param dataSourceMetadataList dataSourceMetadataList.
   */
  private void validateDataSourceMetadata(List<DataSourceMetadata> dataSourceMetadataList) {

    Set<String> reviewedDataSources = new HashSet<>();
    for (DataSourceMetadata dataSourceMetadata : dataSourceMetadataList) {

      if (StringUtils.isEmpty(dataSourceMetadata.getName())) {
        throw new IllegalArgumentException(
            "Missing Name Field for a dataSource. Name is a required parameter.");
      }

      if (!dataSourceMetadata.getName().matches(DATASOURCE_NAME_REGEX)) {
        throw new IllegalArgumentException(
            String.format("DataSource Name: %s contains illegal characters."
            + " Allowed characters: a-zA-Z0-9_-*@ ", dataSourceMetadata.getName()));
      }

      String dataSourceName = dataSourceMetadata.getName();
      if (reviewedDataSources.contains(dataSourceName)) {
        throw new IllegalArgumentException("DataSources with same name are not allowed.");
      } else {
        reviewedDataSources.add(dataSourceName);
      }

      if (Objects.isNull(dataSourceMetadata.getProperties())) {
        throw new IllegalArgumentException("Missing properties field in dataSource configuration."
            + "Properties are required parameters");
      }

    }
  }

  // TODO: for now register storage engine functions here which should be static per storage engine
  private void registerFunctions(String catalogName, StorageEngine storageEngine) {
    storageEngine.getFunctions()
        .forEach(functionResolver ->
            BuiltinFunctionRepository.getInstance().register(catalogName, functionResolver));
  }


}

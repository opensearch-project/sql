/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import static org.opensearch.sql.analysis.DatasourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
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
import org.opensearch.sql.datasource.DatasourceService;
import org.opensearch.sql.datasource.model.Datasource;
import org.opensearch.sql.datasource.model.DatasourceMetadata;
import org.opensearch.sql.datasource.model.ConnectorType;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.prometheus.storage.PrometheusStorageFactory;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.StorageEngineFactory;

/**
 * This class manages datasources and responsible for creating connectors to these datasources.
 */
public class DatasourceServiceImpl implements DatasourceService {

  private static final DatasourceServiceImpl INSTANCE = new DatasourceServiceImpl();

  private static final String CATALOG_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";

  private static final Logger LOG = LogManager.getLogger();

  private Map<String, Datasource> datasourceMap = new HashMap<>();

  private final Map<ConnectorType, StorageEngineFactory> connectorTypeStorageEngineFactoryMap;

  public static DatasourceServiceImpl getInstance() {
    return INSTANCE;
  }

  private DatasourceServiceImpl() {
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
    doPrivileged(() -> {
      InputStream inputStream = DatasourceSettings.DATASOURCE_CONFIG.get(settings);
      if (inputStream != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
          List<DatasourceMetadata> datasourceMetadataList =
              objectMapper.readValue(inputStream, new TypeReference<>() {
              });
          validateDatasources(datasourceMetadataList);
          constructConnectors(datasourceMetadataList);
        } catch (IOException e) {
          LOG.error("Datasource Configuration File uploaded is malformed. Verify and re-upload.", e);
        } catch (Throwable e) {
          LOG.error("Datasource construction failed.", e);
        }
      }
      return null;
    });
  }

  @Override
  public Set<Datasource> getDatasources() {
    return new HashSet<>(datasourceMap.values());
  }

  @Override
  public Datasource getDatasource(String datasourceName) {
    if (!datasourceMap.containsKey(datasourceName)) {
      throw new IllegalArgumentException(
          String.format("Datasource with name %s doesn't exist.", datasourceName));
    }
    return datasourceMap.get(datasourceName);
  }


  @Override
  public void registerDefaultOpenSearchDatasource(StorageEngine storageEngine) {
    if (storageEngine == null) {
      throw new IllegalArgumentException("Default storage engine can't be null");
    }
    datasourceMap.put(DEFAULT_DATASOURCE_NAME,
        new Datasource(DEFAULT_DATASOURCE_NAME, ConnectorType.OPENSEARCH, storageEngine));
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }

  private StorageEngine createStorageEngine(DatasourceMetadata catalog) {
    ConnectorType connector = catalog.getConnector();
    switch (connector) {
      case PROMETHEUS:
        return connectorTypeStorageEngineFactoryMap
            .get(catalog.getConnector())
            .getStorageEngine(catalog.getName(), catalog.getProperties());
      default:
        throw new IllegalStateException(
            String.format("Unsupported Connector: %s", connector.name()));
    }
  }

  private void constructConnectors(List<DatasourceMetadata> datasourceMetadataList) {
    datasourceMap = new HashMap<>();
    for (DatasourceMetadata datasourceMetadata : datasourceMetadataList) {
      try {
        String catalogName = datasourceMetadata.getName();
        StorageEngine storageEngine = createStorageEngine(datasourceMetadata);
        datasourceMap.put(catalogName,
            new Datasource(datasourceMetadata.getName(), datasourceMetadata.getConnector(),
                storageEngine));
      } catch (Throwable e) {
        LOG.error("Catalog : {} storage engine creation failed with the following message: {}",
            datasourceMetadata.getName(), e.getMessage(), e);
      }
    }
  }

  /**
   * This can be moved to a different validator class
   * when we introduce more connectors.
   *
   * @param datasourceMetadataList datasourceMetadataList.
   */
  private void validateDatasources(List<DatasourceMetadata> datasourceMetadataList) {

    Set<String> reviewedCatalogs = new HashSet<>();
    for (DatasourceMetadata datasourceMetadata : datasourceMetadataList) {

      if (StringUtils.isEmpty(datasourceMetadata.getName())) {
        throw new IllegalArgumentException(
            "Missing Name Field from a datasourceMetadata. Name is a required parameter.");
      }

      if (!datasourceMetadata.getName().matches(CATALOG_NAME_REGEX)) {
        throw new IllegalArgumentException(
            String.format("Catalog Name: %s contains illegal characters."
            + " Allowed characters: a-zA-Z0-9_-*@ ", datasourceMetadata.getName()));
      }

      String catalogName = datasourceMetadata.getName();
      if (reviewedCatalogs.contains(catalogName)) {
        throw new IllegalArgumentException("Catalogs with same name are not allowed.");
      } else {
        reviewedCatalogs.add(catalogName);
      }

      if (Objects.isNull(datasourceMetadata.getProperties())) {
        throw new IllegalArgumentException("Missing properties field in datasourceMetadata configuration. "
            + "Properties are required parameters");
      }

    }
  }


}

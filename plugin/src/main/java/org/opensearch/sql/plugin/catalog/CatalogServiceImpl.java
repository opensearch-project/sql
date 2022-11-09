/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

import static org.opensearch.sql.analysis.CatalogSchemaIdentifierNameResolver.DEFAULT_CATALOG_NAME;

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
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.catalog.model.Catalog;
import org.opensearch.sql.catalog.model.CatalogMetadata;
import org.opensearch.sql.catalog.model.ConnectorType;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.prometheus.storage.PrometheusStorageFactory;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.StorageEngineFactory;

/**
 * This class manages catalogs and responsible for creating connectors to these catalogs.
 */
public class CatalogServiceImpl implements CatalogService {

  private static final CatalogServiceImpl INSTANCE = new CatalogServiceImpl();

  private static final String CATALOG_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";

  private static final Logger LOG = LogManager.getLogger();

  private Map<String, Catalog> catalogMap = new HashMap<>();

  private final Map<ConnectorType, StorageEngineFactory> connectorTypeStorageEngineFactoryMap;

  public static CatalogServiceImpl getInstance() {
    return INSTANCE;
  }

  private CatalogServiceImpl() {
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
      InputStream inputStream = CatalogSettings.CATALOG_CONFIG.get(settings);
      if (inputStream != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
          List<CatalogMetadata> catalogs =
              objectMapper.readValue(inputStream, new TypeReference<>() {
              });
          validateCatalogs(catalogs);
          constructConnectors(catalogs);
        } catch (IOException e) {
          LOG.error("Catalog Configuration File uploaded is malformed. Verify and re-upload.", e);
        } catch (Throwable e) {
          LOG.error("Catalog construction failed.", e);
        }
      }
      return null;
    });
  }

  @Override
  public Set<Catalog> getCatalogs() {
    return new HashSet<>(catalogMap.values());
  }

  @Override
  public Catalog getCatalog(String catalogName) {
    if (!catalogMap.containsKey(catalogName)) {
      throw new IllegalArgumentException(
          String.format("Catalog with name %s doesn't exist.", catalogName));
    }
    return catalogMap.get(catalogName);
  }


  @Override
  public void registerDefaultOpenSearchCatalog(StorageEngine storageEngine) {
    if (storageEngine == null) {
      throw new IllegalArgumentException("Default storage engine can't be null");
    }
    catalogMap.put(DEFAULT_CATALOG_NAME,
        new Catalog(DEFAULT_CATALOG_NAME, ConnectorType.OPENSEARCH, storageEngine));
    registerFunctions(DEFAULT_CATALOG_NAME, storageEngine);
  }

  private StorageEngine createStorageEngine(CatalogMetadata catalog) {
    ConnectorType connector = catalog.getConnector();
    switch (connector) {
      case PROMETHEUS:
        StorageEngine storageEngine = connectorTypeStorageEngineFactoryMap
            .get(catalog.getConnector())
            .getStorageEngine(catalog.getName(), catalog.getProperties());
        registerFunctions(catalog.getName(), storageEngine);
        return storageEngine;
      default:
        throw new IllegalStateException(
            String.format("Unsupported Connector: %s", connector.name()));
    }
  }

  private void constructConnectors(List<CatalogMetadata> catalogs) {
    catalogMap = new HashMap<>();
    for (CatalogMetadata catalog : catalogs) {
      try {
        String catalogName = catalog.getName();
        StorageEngine storageEngine = createStorageEngine(catalog);
        catalogMap.put(catalogName,
            new Catalog(catalog.getName(), catalog.getConnector(), storageEngine));
      } catch (Throwable e) {
        LOG.error("Catalog : {} storage engine creation failed with the following message: {}",
            catalog.getName(), e.getMessage(), e);
      }
    }
  }

  /**
   * This can be moved to a different validator class
   * when we introduce more connectors.
   *
   * @param catalogs catalogs.
   */
  private void validateCatalogs(List<CatalogMetadata> catalogs) {

    Set<String> reviewedCatalogs = new HashSet<>();
    for (CatalogMetadata catalog : catalogs) {

      if (StringUtils.isEmpty(catalog.getName())) {
        throw new IllegalArgumentException(
            "Missing Name Field from a catalog. Name is a required parameter.");
      }

      if (!catalog.getName().matches(CATALOG_NAME_REGEX)) {
        throw new IllegalArgumentException(
            String.format("Catalog Name: %s contains illegal characters."
            + " Allowed characters: a-zA-Z0-9_-*@ ", catalog.getName()));
      }

      String catalogName = catalog.getName();
      if (reviewedCatalogs.contains(catalogName)) {
        throw new IllegalArgumentException("Catalogs with same name are not allowed.");
      } else {
        reviewedCatalogs.add(catalogName);
      }

      if (Objects.isNull(catalog.getProperties())) {
        throw new IllegalArgumentException("Missing properties field in catalog configuration. "
            + "Properties are required parameters");
      }

    }
  }

  private void registerFunctions(String catalogName, StorageEngine storageEngine) {
    storageEngine.getFunctions()
        .forEach(functionResolver ->
            BuiltinFunctionRepository.getInstance().register(catalogName, functionResolver));
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.catalog.model.CatalogMetadata;
import org.opensearch.sql.catalog.model.ConnectorType;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.client.PrometheusClientImpl;
import org.opensearch.sql.prometheus.storage.PrometheusStorageEngine;
import org.opensearch.sql.storage.StorageEngine;

/**
 * This class manages catalogs and responsible for creating connectors to these catalogs.
 */
public class CatalogServiceImpl implements CatalogService {

  private static final CatalogServiceImpl INSTANCE = new CatalogServiceImpl();

  private static final String CATALOG_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";

  private static final Logger LOG = LogManager.getLogger();

  public static StorageEngine defaultOpenSearchStorageEngine;

  private Map<String, StorageEngine> storageEngineMap = new HashMap<>();

  public static CatalogServiceImpl getInstance() {
    return INSTANCE;
  }

  private CatalogServiceImpl() {
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
          LOG.info(catalogs.toString());
          validateCatalogs(catalogs);
          constructConnectors(catalogs);
        } catch (IOException e) {
          LOG.error("Catalog Configuration File uploaded is malformed. Verify and re-upload.");
          throw new IllegalArgumentException(
              "Malformed Catalog Configuration Json" + e.getMessage());
        }
      }
      return null;
    });
  }

  @Override
  public StorageEngine getStorageEngine(String catalog) {
    if (!storageEngineMap.containsKey(catalog)) {
      return defaultOpenSearchStorageEngine;
    }
    return storageEngineMap.get(catalog);
  }

  @Override
  public Set<String> getCatalogs() {
    return Collections.unmodifiableSet(storageEngineMap.keySet());
  }

  @Override
  public void registerOpenSearchStorageEngine(StorageEngine storageEngine) {
    if (storageEngine == null) {
      throw new IllegalArgumentException("Default storage engine can't be null");
    }
    defaultOpenSearchStorageEngine = storageEngine;
  }

  private StorageEngine createStorageEngine(CatalogMetadata catalog) throws URISyntaxException {
    StorageEngine storageEngine;
    ConnectorType connector = catalog.getConnector();
    switch (connector) {
      case PROMETHEUS:
        PrometheusClient
            prometheusClient =
            new PrometheusClientImpl(new OkHttpClient(),
                new URI(catalog.getUri()));
        storageEngine = new PrometheusStorageEngine(prometheusClient);
        break;
      default:
        LOG.info(
            "Unknown connector \"{}\". "
                + "Please re-upload catalog configuration with a supported connector.",
            connector);
        throw new IllegalStateException(
            "Unknown connector. Connector doesn't exist in the list of supported.");
    }
    return storageEngine;
  }

  private void constructConnectors(List<CatalogMetadata> catalogs) throws URISyntaxException {
    storageEngineMap = new HashMap<>();
    for (CatalogMetadata catalog : catalogs) {
      String catalogName = catalog.getName();
      StorageEngine storageEngine = createStorageEngine(catalog);
      storageEngineMap.put(catalogName, storageEngine);
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
        LOG.error("Found a catalog with no name. {}", catalog.toString());
        throw new IllegalArgumentException(
            "Missing Name Field from a catalog. Name is a required parameter.");
      }

      if (!catalog.getName().matches(CATALOG_NAME_REGEX)) {
        LOG.error(String.format("Catalog Name: %s contains illegal characters."
            + " Allowed characters: a-zA-Z0-9_-*@ ", catalog.getName()));
        throw new IllegalArgumentException(
            String.format("Catalog Name: %s contains illegal characters."
            + " Allowed characters: a-zA-Z0-9_-*@ ", catalog.getName()));
      }

      if (StringUtils.isEmpty(catalog.getUri())) {
        LOG.error("Found a catalog with no uri. {}", catalog.toString());
        throw new IllegalArgumentException(
            "Missing URI Field from a catalog. URI is a required parameter.");
      }

      String catalogName = catalog.getName();
      if (reviewedCatalogs.contains(catalogName)) {
        LOG.error("Found duplicate catalog names");
        throw new IllegalArgumentException("Catalogs with same name are not allowed.");
      } else {
        reviewedCatalogs.add(catalogName);
      }
    }
  }


}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.client.PrometheusClientImpl;
import org.opensearch.sql.prometheus.config.PrometheusConfig;
import org.opensearch.sql.prometheus.storage.PrometheusStorageEngine;
import org.opensearch.sql.storage.StorageEngine;

/**
 * This class manages catalogs and responsible for creating connectors to these catalogs.
 */
public class CatalogServiceImpl implements CatalogService {


  private final Set<String> allowedConnectors = new HashSet<>() {{
      add("prometheus");
    }};

  private Map<String, StorageEngine> storageEngineMap = new HashMap<>();

  private static final CatalogServiceImpl INSTANCE = new CatalogServiceImpl();

  private static final Logger LOG = LogManager.getLogger();

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
    doPrivileged(() -> {
      Boolean isPPLEnabled = (Boolean) OpenSearchSettings.PPL_ENABLED_SETTING.get(settings);
      Boolean isFederationEnabled = CatalogSettings.FEDERATION_ENABLED.get(settings);
      if (isPPLEnabled && isFederationEnabled) {
        InputStream inputStream = CatalogSettings.CATALOG_CONFIG.get(settings);
        if (inputStream != null) {
          ObjectMapper objectMapper = new ObjectMapper();
          objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
          try {
            ArrayNode catalogs = objectMapper.readValue(inputStream, ArrayNode.class);
            validateCatalogs(catalogs);
            constructConnectors(catalogs);
          } catch (IOException e) {
            LOG.error("Catalog Configuration File uploaded is malformed. Verify and re-upload.");
            throw new IllegalArgumentException(
                "Malformed Catalog Configuration Json" + e.getMessage());
          }
        }
      } else {
        LOG.info("PPL federation is not enabled.");
      }
      return null;
    });
  }

  @Override
  public Optional<StorageEngine> getStorageEngine(String catalog) {
    return Optional.ofNullable(storageEngineMap.get(catalog));
  }

  @Override
  public Set<String> getCatalogs() {
    return storageEngineMap.keySet();
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }

  private StorageEngine createStorageEngine(JsonNode catalog) throws URISyntaxException {
    StorageEngine storageEngine;
    String connector = catalog.get(CatalogConstants.CONNECTOR).asText();
    switch (connector) {
      case "prometheus":
        PrometheusClient
            prometheusClient =
            new PrometheusClientImpl(new OkHttpClient(),
                new URI(catalog.get(CatalogConstants.URI).asText()));
        PrometheusConfig prometheusConfig = new PrometheusConfig();
        if (catalog.has(CatalogConstants.DEFAULT_TIME_RANGE)) {
          prometheusConfig.setDefaultTimeRange(
              catalog.get(CatalogConstants.DEFAULT_TIME_RANGE).asLong());

        }
        storageEngine = new PrometheusStorageEngine(prometheusClient, prometheusConfig);
        break;
      default:
        LOG.info(
            "Unknown connector \"{}\". "
                + "Please re-upload catalog configuration with a supported connector.",
            catalog.get("connector"));
        throw new IllegalStateException(
            "Unknown connector. Connector doesn't exist in the list of supported.");
    }
    return storageEngine;
  }

  private void constructConnectors(ArrayNode catalogs) throws URISyntaxException {
    storageEngineMap = new HashMap<>();
    for (JsonNode catalog : catalogs) {
      String catalogName = catalog.get(CatalogConstants.NAME).asText();
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
  private void validateCatalogs(ArrayNode catalogs) {
    Set<String> reviewedCatalogs = new HashSet<>();
    for (JsonNode catalog : catalogs) {

      if (!catalog.has(CatalogConstants.NAME)
          || StringUtils.isEmpty(catalog.get(CatalogConstants.NAME).asText())) {
        LOG.error("Found a catalog with no name. {}", catalog.toPrettyString());
        throw new IllegalArgumentException(
            "Missing Name Field from a catalog. Name is a required parameter.");
      }

      if (!catalog.has(CatalogConstants.URI)
          || StringUtils.isEmpty(catalog.get(CatalogConstants.URI).asText())) {
        LOG.error("Found a catalog with no uri. {}", catalog.toPrettyString());
        throw new IllegalArgumentException(
            "Missing URI Field from a catalog. URI is a required parameter.");
      }

      if (!catalog.has(CatalogConstants.CONNECTOR)
          || StringUtils.isEmpty(catalog.get(CatalogConstants.CONNECTOR).asText())) {
        LOG.error("Found a catalog with no connector . {}", catalog.toPrettyString());
        throw new IllegalArgumentException(
            "Missing connector Field from a catalog. connector is a required parameter.");
      }

      String catalogName = catalog.get(CatalogConstants.NAME).asText();
      if (reviewedCatalogs.contains(catalogName)) {
        LOG.error("Found duplicate catalog names");
        throw new IllegalArgumentException("Catalogs with same name are not allowed.");
      } else {
        reviewedCatalogs.add(catalogName);
      }

      String connector = catalog.get(CatalogConstants.CONNECTOR).asText();
      if (!allowedConnectors.contains(connector)) {
        LOG.error("Found catalog with unsupported connector type: {}", catalog.toPrettyString());
        throw new IllegalArgumentException("Found catalog with unsupported connector type."
            + allowedConnectors + ": are the only allowed connector types");
      }


    }
  }

}
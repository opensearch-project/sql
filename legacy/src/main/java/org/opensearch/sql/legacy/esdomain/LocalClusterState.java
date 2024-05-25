/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.esdomain;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.esdomain.mapping.IndexMappings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

/**
 * Local cluster state information which may be stale but help avoid blocking operation in NIO
 * thread.
 *
 * <ol>
 *   <li>Why extending TransportAction doesn't work here? TransportAction enforce implementation to
 *       be performed remotely but local cluster state read is expected here.
 *   <li>Why injection by AbstractModule doesn't work here? Because this state needs to be used
 *       across the plugin, ex. in rewriter, pretty formatter etc.
 * </ol>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LocalClusterState {

  private static final Logger LOG = LogManager.getLogger();

  /** Singleton instance */
  private static LocalClusterState INSTANCE;

  /** Current cluster state on local node */
  private ClusterService clusterService;

  private Client client;

  private OpenSearchSettings pluginSettings;

  /** Latest setting value for each registered key. Thread-safe is required. */
  private final Map<String, Object> latestSettings = new ConcurrentHashMap<>();

  public static synchronized LocalClusterState state() {
    if (INSTANCE == null) {
      INSTANCE = new LocalClusterState();
    }
    return INSTANCE;
  }

  /** Give testing code a chance to inject mock object */
  public static synchronized void state(LocalClusterState instance) {
    INSTANCE = instance;
  }

  /**
   * Sets the ClusterService used to receive ClusterSetting update notifications.
   *
   * @param clusterService The non-null cluster service instance.
   */
  public void setClusterService(@NonNull ClusterService clusterService) {
    this.clusterService = clusterService;
  }

  /**
   * Sets the Client used to interact with OpenSearch core.
   *
   * @param client The non-null client instance
   */
  public void setClient(@NonNull Client client) {
    this.client = client;
  }

  /**
   * Sets the plugin's settings.
   *
   * @param settings The non-null plugin settings instance
   */
  public void setPluginSettings(@NonNull OpenSearchSettings settings) {

    this.pluginSettings = settings;

    for (Setting<?> setting : settings.getSettings()) {
      clusterService
          .getClusterSettings()
          .addSettingsUpdateConsumer(
              setting,
              newVal -> {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("The value of setting [{}] changed to [{}]", setting.getKey(), newVal);
                }
                latestSettings.put(setting.getKey(), newVal);
              });
    }
  }

  /**
   * Get plugin setting value by key. Return default value if not configured explicitly.
   *
   * @param key setting key registered during plugin bootstrap.
   * @return setting value or default.
   */
  @SuppressWarnings("unchecked")
  public <T> T getSettingValue(Settings.Key key) {
    Objects.requireNonNull(pluginSettings, "SQL plugin setting is null");
    return (T) latestSettings.getOrDefault(key.getKeyValue(), pluginSettings.getSettingValue(key));
  }

  /**
   * Get field mappings by index expressions. Because IndexMetaData/MappingMetaData is hard to
   * convert to FieldMappingMetaData, custom mapping domain objects are being used here. In future,
   * it should be moved to domain model layer for all OpenSearch specific knowledge.
   *
   * @param indices index name expression
   * @return index mapping(s)
   */
  public IndexMappings getFieldMappings(String[] indices) {
    Objects.requireNonNull(client, "Client is null");

    try {

      Map<String, MappingMetadata> mappingMetadata =
          client
              .admin()
              .indices()
              .prepareGetMappings(indices)
              .setLocal(true)
              .setIndicesOptions(IndicesOptions.strictExpandOpen())
              .execute()
              .actionGet(0, TimeUnit.NANOSECONDS)
              .mappings();

      IndexMappings mappings = new IndexMappings(mappingMetadata);

      LOG.debug("Found mappings: {}", mappings);
      return mappings;
    } catch (IndexNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read mapping in cluster state for indices=" + Arrays.toString(indices), e);
    }
  }
}

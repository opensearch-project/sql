/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.esdomain;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.esdomain.mapping.IndexMappings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

/**
 * Local cluster state information which may be stale but help avoid blocking operation in NIO thread.
 * <p>
 * 1) Why extending TransportAction doesn't work here?
 * TransportAction enforce implementation to be performed remotely but local cluster state read is expected here.
 * <p>
 * 2) Why injection by AbstractModule doesn't work here?
 * Because this state needs to be used across the plugin, ex. in rewriter, pretty formatter etc.
 */
public class LocalClusterState {

    private static final Logger LOG = LogManager.getLogger();

    private static final Function<String, Predicate<String>> ALL_FIELDS = (anyIndex -> (anyField -> true));

    /**
     * Singleton instance
     */
    private static LocalClusterState INSTANCE;

    /**
     * Current cluster state on local node
     */
    private ClusterService clusterService;

    private OpenSearchSettings pluginSettings;

    /**
     * Index name expression resolver to get concrete index name
     */
    private IndexNameExpressionResolver resolver;

    /**
     * Thread-safe mapping cache to save the computation of sourceAsMap() which is not lightweight as thought
     * Array cannot be used as key because hashCode() always return reference address, so either use wrapper or List.
     */
    private final Cache<List<String>, IndexMappings> cache;

    /**
     * Latest setting value for each registered key. Thread-safe is required.
     */
    private final Map<String, Object> latestSettings = new ConcurrentHashMap<>();

    public static synchronized LocalClusterState state() {
        if (INSTANCE == null) {
            INSTANCE = new LocalClusterState();
        }
        return INSTANCE;
    }

    /**
     * Give testing code a chance to inject mock object
     */
    public static synchronized void state(LocalClusterState instance) {
        INSTANCE = instance;
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;

        clusterService.addListener(event -> {
            if (event.metadataChanged()) {
                // State in cluster service is already changed to event.state() before listener fired
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Metadata in cluster state changed: {}",
                            new IndexMappings(clusterService.state().metadata()));
                }
                cache.invalidateAll();
            }
        });
    }

    public void setPluginSettings(OpenSearchSettings settings) {
        this.pluginSettings = settings;
        for (Setting<?> setting: settings.getSettings()) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(
                setting,
                newVal -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("The value of setting [{}] changed to [{}]", setting.getKey(), newVal);
                    }
                    latestSettings.put(setting.getKey(), newVal);
                }
            );
        }

    }

    public void setResolver(IndexNameExpressionResolver resolver) {
        this.resolver = resolver;
    }

    private LocalClusterState() {
        cache = CacheBuilder.newBuilder().maximumSize(100).build();
    }

    /**
     * Get plugin setting value by key. Return default value if not configured explicitly.
     * @param key setting key registered during plugin bootstrap.
     * @return setting value or default.
     */
    @SuppressWarnings("unchecked")
    public <T> T getSettingValue(Settings.Key key) {
        Objects.requireNonNull(pluginSettings, "SQL plugin setting is null");
        return (T) latestSettings.getOrDefault(key.getKeyValue(),
            pluginSettings.getSettingValue(key));
    }

    /**
     * Get field mappings by index expressions. All types and fields are included in response.
     */
    public IndexMappings getFieldMappings(String[] indices) {
        return getFieldMappings(indices, ALL_FIELDS);
    }

    /**
     * Get field mappings by index expressions, type and field filter. Because IndexMetaData/MappingMetaData
     * is hard to convert to FieldMappingMetaData, custom mapping domain objects are being used here. In future,
     * it should be moved to domain model layer for all OpenSearch specific knowledge.
     * <p>
     * Note that cluster state may be change inside OpenSearch so it's possible to read different state in 2 accesses
     * to ClusterService.state() here.
     *
     * @param indices     index name expression
     * @param fieldFilter field filter predicate
     * @return index mapping(s)
     */
    private IndexMappings getFieldMappings(String[] indices, Function<String, Predicate<String>> fieldFilter) {
        Objects.requireNonNull(clusterService, "Cluster service is null");
        Objects.requireNonNull(resolver, "Index name expression resolver is null");

        try {
            ClusterState state = clusterService.state();
            String[] concreteIndices = resolveIndexExpression(state, indices);

            IndexMappings mappings;
            if (fieldFilter == ALL_FIELDS) {
                mappings = findMappingsInCache(state, concreteIndices);
            } else {
                mappings = findMappings(state, concreteIndices, fieldFilter);
            }

            LOG.debug("Found mappings: {}", mappings);
            return mappings;
        } catch (IndexNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to read mapping in cluster state for indices="
                            + Arrays.toString(indices) , e);
        }
    }

    private String[] resolveIndexExpression(ClusterState state, String[] indices) {
        String[] concreteIndices = resolver.concreteIndexNames(state, IndicesOptions.strictExpandOpen(), true, indices);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Resolved index expression {} to concrete index names {}",
                    Arrays.toString(indices), Arrays.toString(concreteIndices));
        }
        return concreteIndices;
    }

    private IndexMappings findMappings(ClusterState state, String[] indices,
                                       Function<String, Predicate<String>> fieldFilter) throws IOException {
        LOG.debug("Cache didn't help. Load and parse mapping in cluster state");
        return new IndexMappings(
                state.metadata().findMappings(indices, fieldFilter)
        );
    }

    private IndexMappings findMappingsInCache(ClusterState state, String[] indices)
            throws ExecutionException {
        LOG.debug("Looking for mapping in cache: {}", cache.asMap());
        return cache.get(sortToList(indices),
                () -> findMappings(state, indices, ALL_FIELDS)
        );
    }

    private <T> List<T> sortToList(T[] array) {
        // Mostly array has single element
        Arrays.sort(array);
        return Arrays.asList(array);
    }

}

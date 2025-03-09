/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.setting;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Setting. */
public abstract class Settings {
  @RequiredArgsConstructor
  public enum Key {

    /** SQL Settings. */
    SQL_ENABLED("plugins.sql.enabled"),
    SQL_SLOWLOG("plugins.sql.slowlog"),
    SQL_CURSOR_KEEP_ALIVE("plugins.sql.cursor.keep_alive"),
    SQL_DELETE_ENABLED("plugins.sql.delete.enabled"),
    SQL_PAGINATION_API_SEARCH_AFTER("plugins.sql.pagination.api"),

    /** PPL Settings. */
    PPL_ENABLED("plugins.ppl.enabled"),

    /** Enable Calcite as execution engine */
    CALCITE_ENGINE_ENABLED("plugins.calcite.enabled"),
    CALCITE_FALLBACK_ALLOWED("plugins.calcite.fallback.allowed"),
    CALCITE_PUSHDOWN_ENABLED("plugins.calcite.pushdown.enabled"),
    CALCITE_LEGACY_ENABLED("plugins.calcite.legacy.enabled"),

    /** Query Settings. */
    FIELD_TYPE_TOLERANCE("plugins.query.field_type_tolerance"),

    /** Common Settings for SQL and PPL. */
    QUERY_MEMORY_LIMIT("plugins.query.memory_limit"),
    QUERY_SIZE_LIMIT("plugins.query.size_limit"),
    ENCYRPTION_MASTER_KEY("plugins.query.datasources.encryption.masterkey"),
    DATASOURCES_URI_HOSTS_DENY_LIST("plugins.query.datasources.uri.hosts.denylist"),
    DATASOURCES_LIMIT("plugins.query.datasources.limit"),
    DATASOURCES_ENABLED("plugins.query.datasources.enabled"),

    METRICS_ROLLING_WINDOW("plugins.query.metrics.rolling_window"),
    METRICS_ROLLING_INTERVAL("plugins.query.metrics.rolling_interval"),
    SPARK_EXECUTION_ENGINE_CONFIG("plugins.query.executionengine.spark.config"),
    CLUSTER_NAME("cluster.name"),
    SPARK_EXECUTION_SESSION_LIMIT("plugins.query.executionengine.spark.session.limit"),
    SPARK_EXECUTION_REFRESH_JOB_LIMIT("plugins.query.executionengine.spark.refresh_job.limit"),
    SESSION_INDEX_TTL("plugins.query.executionengine.spark.session.index.ttl"),
    RESULT_INDEX_TTL("plugins.query.executionengine.spark.result.index.ttl"),
    AUTO_INDEX_MANAGEMENT_ENABLED(
        "plugins.query.executionengine.spark.auto_index_management.enabled"),
    SESSION_INACTIVITY_TIMEOUT_MILLIS(
        "plugins.query.executionengine.spark.session_inactivity_timeout_millis"),

    /** Async query Settings * */
    ASYNC_QUERY_ENABLED("plugins.query.executionengine.async_query.enabled"),
    ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED(
        "plugins.query.executionengine.async_query.external_scheduler.enabled"),
    ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL(
        "plugins.query.executionengine.async_query.external_scheduler.interval"),
    STREAMING_JOB_HOUSEKEEPER_INTERVAL(
        "plugins.query.executionengine.spark.streamingjobs.housekeeper.interval");

    @Getter private final String keyValue;

    private static final Map<String, Key> ALL_KEYS;

    static {
      ImmutableMap.Builder<String, Key> builder = new ImmutableMap.Builder<>();
      for (Key key : Key.values()) {
        builder.put(key.getKeyValue(), key);
      }
      ALL_KEYS = builder.build();
    }

    public static Optional<Key> of(String keyValue) {
      String key = Strings.isNullOrEmpty(keyValue) ? "" : keyValue.toLowerCase();
      return Optional.ofNullable(ALL_KEYS.getOrDefault(key, null));
    }
  }

  /** Get Setting Value. */
  public abstract <T> T getSettingValue(Key key);

  public abstract List<?> getSettings();
}

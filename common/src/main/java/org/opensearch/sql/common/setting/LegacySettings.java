/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.setting;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Legacy Open Distro Settings. */
public abstract class LegacySettings {
  @RequiredArgsConstructor
  public enum Key {

    /** Legacy SQL Settings. */
    SQL_ENABLED("opendistro.sql.enabled"),
    SQL_QUERY_SLOWLOG("opendistro.sql.query.slowlog"),
    SQL_CURSOR_KEEPALIVE("opendistro.sql.cursor.keep_alive"),
    METRICS_ROLLING_WINDOW("opendistro.sql.metrics.rollingwindow"),
    METRICS_ROLLING_INTERVAL("opendistro.sql.metrics.rollinginterval"),

    /** Legacy PPL Settings. */
    PPL_ENABLED("opendistro.ppl.enabled"),
    PPL_QUERY_MEMORY_LIMIT("opendistro.ppl.query.memory_limit"),

    /** Legacy Common Settings. */
    QUERY_SIZE_LIMIT("opendistro.query.size_limit"),

    /** Deprecated Settings. */
    SQL_NEW_ENGINE_ENABLED("opendistro.sql.engine.new.enabled"),
    QUERY_ANALYSIS_ENABLED("opendistro.sql.query.analysis.enabled"),
    QUERY_ANALYSIS_SEMANTIC_SUGGESTION("opendistro.sql.query.analysis.semantic.suggestion"),
    QUERY_ANALYSIS_SEMANTIC_THRESHOLD("opendistro.sql.query.analysis.semantic.threshold"),
    QUERY_RESPONSE_FORMAT("opendistro.sql.query.response.format"),
    SQL_CURSOR_ENABLED("opendistro.sql.cursor.enabled"),
    SQL_CURSOR_FETCH_SIZE("opendistro.sql.cursor.fetch_size");

    @Getter private final String keyValue;
  }

  public abstract <T> T getSettingValue(Key key);
}

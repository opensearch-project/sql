/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.plugin;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.NodeScope;
import static org.opensearch.common.unit.TimeValue.timeValueMinutes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.common.settings.Setting;
import org.opensearch.sql.legacy.executor.Format;

/**
 * SQL plugin settings
 */
public class SqlSettings {

    /**
     * Get plugin settings stored in cluster setting. Why not use OpenSearch slow log settings consistently?
     * 1) It's per-index setting.
     * 2) It has separate setting for Query and Fetch phase which are all OpenSearch internal concepts.
     */
    public static final String SQL_ENABLED = "opensearch.sql.enabled";
    public static final String QUERY_SLOWLOG = "opensearch.sql.query.slowlog";
    public static final String METRICS_ROLLING_WINDOW = "opensearch.sql.metrics.rollingwindow";
    public static final String METRICS_ROLLING_INTERVAL = "opensearch.sql.metrics.rollinginterval";

    public static final String CURSOR_KEEPALIVE= "opensearch.sql.cursor.keep_alive";

    public static final String DEFAULT_RESPONSE_FORMAT = Format.JDBC.getFormatName();

    private final Map<String, Setting<?>> settings;

    public SqlSettings() {
        Map<String, Setting<?>> settings = new HashMap<>();
        settings.put(SQL_ENABLED, Setting.boolSetting(SQL_ENABLED, true, NodeScope, Dynamic));
        settings.put(QUERY_SLOWLOG, Setting.intSetting(QUERY_SLOWLOG, 2, NodeScope, Dynamic));

        settings.put(METRICS_ROLLING_WINDOW, Setting.longSetting(METRICS_ROLLING_WINDOW, 3600L, 2L,
                NodeScope, Dynamic));
        settings.put(METRICS_ROLLING_INTERVAL, Setting.longSetting(METRICS_ROLLING_INTERVAL, 60L, 1L,
                NodeScope, Dynamic));

        // Settings for cursor
        settings.put(CURSOR_KEEPALIVE, Setting.positiveTimeSetting(CURSOR_KEEPALIVE, timeValueMinutes(1),
                NodeScope, Dynamic));

        this.settings = unmodifiableMap(settings);
    }

    public SqlSettings(Map<String, Setting<?>> settings) {
        this.settings = unmodifiableMap(settings);
    }

    public Setting<?> getSetting(String key) {
        if (settings.containsKey(key)) {
            return settings.get(key);
        }
        throw new IllegalArgumentException("Cannot find setting by key [" + key + "]");
    }

    public List<Setting<?>> getSettings() {
        return new ArrayList<>(settings.values());
    }

}

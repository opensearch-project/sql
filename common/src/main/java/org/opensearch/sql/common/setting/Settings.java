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
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.common.setting;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Setting.
 */
public abstract class Settings {
  @RequiredArgsConstructor
  public enum Key {

    /**
     * SQL Settings.
     */
    SQL_ENABLED("plugins.sql.enabled"),
    SQL_SLOWLOG("plugins.sql.slowlog"),
    SQL_CURSOR_KEEP_ALIVE("plugins.sql.cursor.keep_alive"),

    /**
     * PPL Settings.
     */
    PPL_ENABLED("plugins.ppl.enabled"),

    /**
     * Common Settings for SQL and PPL.
     */
    QUERY_MEMORY_LIMIT("plugins.query.memory_limit"),
    QUERY_SIZE_LIMIT("plugins.query.size_limit"),
    METRICS_ROLLING_WINDOW("plugins.query.metrics.rolling_window"),
    METRICS_ROLLING_INTERVAL("plugins.query.metrics.rolling_interval");

    @Getter
    private final String keyValue;

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

  /**
   * Get Setting Value.
   */
  public abstract <T> T getSettingValue(Key key);

  public abstract List<?> getSettings();
}

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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Legacy Open Distro Settings.
 */
public abstract class LegacySettings {
  @RequiredArgsConstructor
  public enum Key {

    /**
     * Legacy SQL Settings.
     */
    SQL_ENABLED("opendistro.sql.enabled"),
    SQL_QUERY_SLOWLOG("opendistro.sql.query.slowlog"),
    METRICS_ROLLING_WINDOW("opendistro.sql.metrics.rollingwindow"),
    METRICS_ROLLING_INTERVAL("opendistro.sql.metrics.rollinginterval"),

    /**
     * Legacy PPL Settings.
     */
    PPL_ENABLED("opendistro.ppl.enabled"),
    PPL_QUERY_MEMORY_LIMIT("opendistro.ppl.query.memory_limit"),

    /**
     * Legacy Common Settings.
     */
    QUERY_SIZE_LIMIT("opendistro.query.size_limit");

    @Getter
    private final String keyValue;
  }

  public abstract <T> T getSettingValue(Key key);
}

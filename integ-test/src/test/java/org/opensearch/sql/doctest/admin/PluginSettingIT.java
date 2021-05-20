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

package org.opensearch.sql.doctest.admin;

import static org.opensearch.common.settings.Setting.Property;
import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.Final;
import static org.opensearch.common.settings.Setting.Property.IndexScope;
import static org.opensearch.common.settings.Setting.Property.NodeScope;
import static org.opensearch.common.settings.Settings.EMPTY;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.CURL_REQUEST;
import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.IGNORE_REQUEST;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.IGNORE_RESPONSE;
import static org.opensearch.sql.doctest.core.response.SqlResponseFormat.PRETTY_JSON_RESPONSE;
import static org.opensearch.sql.legacy.plugin.SqlSettings.CURSOR_KEEPALIVE;
import static org.opensearch.sql.legacy.plugin.SqlSettings.QUERY_SLOWLOG;
import static org.opensearch.sql.legacy.plugin.SqlSettings.SQL_ENABLED;

import java.util.EnumSet;
import org.opensearch.common.settings.Setting;
import org.opensearch.sql.doctest.core.DocTest;
import org.opensearch.sql.doctest.core.annotation.DocTestConfig;
import org.opensearch.sql.doctest.core.annotation.Section;
import org.opensearch.sql.doctest.core.builder.Example;
import org.opensearch.sql.doctest.core.builder.ListItems;
import org.opensearch.sql.legacy.plugin.SqlSettings;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Doc test for plugin settings.
 */
@DocTestConfig(template = "admin/settings.rst", testData = {"accounts.json"})
public class PluginSettingIT extends DocTest {

  private static final SqlSettings SETTINGS = new SqlSettings();

  @Section(1)
  public void sqlEnabledSetting() {
    docSetting(
        SQL_ENABLED,
        "You can disable SQL plugin to reject all coming requests.",
        false,
        "SELECT * FROM accounts"
    );
  }

  @Section(2)
  public void slowLogSetting() {
    docSetting(
        QUERY_SLOWLOG,
        "You can configure the time limit (seconds) for slow query which would be logged as " +
            "'Slow query: elapsed=xxx (ms)' in opensearch.log.",
        10
    );
  }

  @Section(9)
  public void cursorDefaultContextKeepAliveSetting() {
    docSetting(
        CURSOR_KEEPALIVE,
        "User can set this value to indicate how long the cursor context should be kept open. " +
            "Cursor contexts are resource heavy, and a lower value should be used if possible.",
        "5m"
    );
  }

  /**
   * Generate content for sample queries with setting changed to new value.
   * Finally setting will be reverted to avoid potential impact on other test cases.
   */
  private void docSetting(String name, String description, Object sampleValue,
                          String... sampleQueries) {
    try {
      section(
          title(name),
          description(description + "\n\n" + listSettingDetails(name)),
          createExamplesForSettingChangeQueryAndOtherSampleQueries(name, sampleValue, sampleQueries)
      );
    } finally {
      // Make sure the change is removed
      try {
        put(name, null).queryResponse();
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  private Example[] createExamplesForSettingChangeQueryAndOtherSampleQueries(String name,
                                                                             Object sampleValue,
                                                                             String[] sampleQueries) {
    Example[] examples = new Example[sampleQueries.length + 1];
    examples[0] = example("You can update the setting with a new value like this.",
        put(name, sampleValue),
        queryFormat(CURL_REQUEST, PRETTY_JSON_RESPONSE),
        explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE));

    for (int i = 0; i < sampleQueries.length; i++) {
      examples[i + 1] = example("Query result after the setting updated is like:",
          post(sampleQueries[i]),
          queryFormat(CURL_REQUEST, PRETTY_JSON_RESPONSE),
          explainFormat(IGNORE_REQUEST, IGNORE_RESPONSE));
    }
    return examples;
  }

  private String listSettingDetails(String name) {
    Setting<?> setting = SETTINGS.getSetting(name);
    ListItems list = new ListItems();

    list.addItem(StringUtils.format("The default value is %s.", setting.getDefault(EMPTY)));

    EnumSet<Property> properties = setting.getProperties();
    if (properties.contains(NodeScope)) {
      list.addItem("This setting is node scope.");
    } else if (properties.contains(IndexScope)) {
      list.addItem("This setting is index scope.");
    }

    if (properties.contains(Dynamic)) {
      list.addItem("This setting can be updated dynamically.");
    } else if (properties.contains(Final)) {
      list.addItem("This setting is not updatable.");
    }
    return list.toString();
  }

}

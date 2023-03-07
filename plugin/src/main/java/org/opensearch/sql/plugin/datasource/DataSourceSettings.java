/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import java.io.InputStream;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;

public class DataSourceSettings {

  public static final Setting<InputStream> DATASOURCE_CONFIG = SecureSetting.secureFile(
      "plugins.query.federation.datasources.config",
      null);

  public static final Setting<String> DATASOURCE_MASTER_SECRET_KEY = Setting.simpleString(
      "plugins.query.datasources.encryption.masterkey",
      "0000000000000000",
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);
}

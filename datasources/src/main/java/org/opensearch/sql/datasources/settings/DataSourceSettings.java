/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.settings;

import java.io.InputStream;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;

public class DataSourceSettings {

  // we are keeping this to not break upgrades if the config is already present.
  // This will be completely removed in 3.0.
  public static final Setting<InputStream> DATASOURCE_CONFIG = SecureSetting.secureFile(
      "plugins.query.federation.datasources.config",
      null,
      Setting.Property.Deprecated);

  public static final Setting<String> DATASOURCE_MASTER_SECRET_KEY = Setting.simpleString(
      "plugins.query.datasources.encryption.masterkey",
      "0000000000000000",
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);
}

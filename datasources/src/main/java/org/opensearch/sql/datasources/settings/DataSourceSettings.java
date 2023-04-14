/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.settings;

import java.io.InputStream;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;

public class DataSourceSettings {

  public static final String DEFAULT_MASTER_KEY = "0000000000000000";

  public static final Setting<InputStream> DATASOURCE_CONFIG = SecureSetting.secureFile(
      "plugins.query.federation.datasources.config",
      null);

  public static final Setting<String> DATASOURCE_INTERFACE = Setting.simpleString(
      "plugins.query.federation.datasources.interface",
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);

  public static final Setting<String> DATASOURCE_MASTER_SECRET_KEY = Setting.simpleString(
      "plugins.query.federation.datasources.encryption.masterkey",
      DEFAULT_MASTER_KEY,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic);
}

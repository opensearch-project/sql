/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.datasource;

import java.io.InputStream;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;

public class DatasourceSettings {

  public static final Setting<InputStream> DATASOURCE_CONFIG = SecureSetting.secureFile(
      "plugins.query.federation.datasources.config",
      null);
}

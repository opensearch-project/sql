/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

import java.io.InputStream;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;

public class CatalogSettings {

  public static final Setting<Boolean> FEDERATION_ENABLED = Setting.boolSetting(
      "plugins.ppl.federation.enabled",
      false,
      Setting.Property.NodeScope,
      Setting.Property.Dynamic
  );

  public static final Setting<InputStream> CATALOG_CONFIG = SecureSetting.secureFile(
      "plugins.ppl.federation.catalog.config",
      null);
}

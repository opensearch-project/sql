/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import org.opensearch.sql.calcite.plan.DynamicFieldsConstants;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

public class PermissiveOpenSearchIndex extends OpenSearchIndex {

  public PermissiveOpenSearchIndex(OpenSearchClient client, Settings settings, String indexName) {
    super(client, settings, indexName);
  }

  @Override
  public Map<String, ExprType> getReservedFieldTypes() {
    Map<String, ExprType> extendedMap = new LinkedHashMap<>(METADATAFIELD_TYPE_MAP);
    extendedMap.put(DynamicFieldsConstants.DYNAMIC_FIELDS_MAP, ExprCoreType.STRUCT);
    return extendedMap;
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.Map;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

public interface OpenSearchConstants {

  String METADATA_FIELD_ID = "_id";
  String METADATA_FIELD_UID = "_uid";
  String METADATA_FIELD_INDEX = "_index";
  String METADATA_FIELD_SCORE = "_score";
  String METADATA_FIELD_MAXSCORE = "_maxscore";
  String METADATA_FIELD_SORT = "_sort";

  String METADATA_FIELD_ROUTING = "_routing";

  java.util.Map<String, ExprType> METADATAFIELD_TYPE_MAP =
      Map.of(
          METADATA_FIELD_ID, ExprCoreType.STRING,
          METADATA_FIELD_UID, ExprCoreType.STRING,
          METADATA_FIELD_INDEX, ExprCoreType.STRING,
          METADATA_FIELD_SCORE, ExprCoreType.FLOAT,
          METADATA_FIELD_MAXSCORE, ExprCoreType.FLOAT,
          METADATA_FIELD_SORT, ExprCoreType.LONG,
          METADATA_FIELD_ROUTING, ExprCoreType.STRING);
}

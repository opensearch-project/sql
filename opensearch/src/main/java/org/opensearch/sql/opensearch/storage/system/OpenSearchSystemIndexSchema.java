/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprType;

/** Definition of the system table schema. */
@Getter
@RequiredArgsConstructor
public enum OpenSearchSystemIndexSchema {
  SYS_TABLE_TABLES(
      new LinkedHashMap<String, ExprType>() {
        {
          put("TABLE_CAT", STRING);
          put("TABLE_SCHEM", STRING);
          put("TABLE_NAME", STRING);
          put("TABLE_TYPE", STRING);
          put("REMARKS", STRING);
          put("TYPE_CAT", STRING);
          put("TYPE_SCHEM", STRING);
          put("TYPE_NAME", STRING);
          put("SELF_REFERENCING_COL_NAME", STRING);
          put("REF_GENERATION", STRING);
        }
      }),
  SYS_TABLE_MAPPINGS(
      new LinkedHashMap<String, ExprType>() {
        {
          put("TABLE_CAT", STRING);
          put("TABLE_SCHEM", STRING);
          put("TABLE_NAME", STRING);
          put("COLUMN_NAME", STRING);
          put("DATA_TYPE", INTEGER);
          put("TYPE_NAME", STRING);
          put("COLUMN_SIZE", INTEGER);
          put("BUFFER_LENGTH", INTEGER);
          put("DECIMAL_DIGITS", INTEGER);
          put("NUM_PREC_RADIX", INTEGER);
          put("NULLABLE", INTEGER);
          put("REMARKS", STRING);
          put("COLUMN_DEF", STRING);
          put("SQL_DATA_TYPE", INTEGER);
          put("SQL_DATETIME_SUB", INTEGER);
          put("CHAR_OCTET_LENGTH", INTEGER);
          put("ORDINAL_POSITION", INTEGER);
          put("IS_NULLABLE", STRING);
          put("SCOPE_CATALOG", STRING);
          put("SCOPE_SCHEMA", STRING);
          put("SCOPE_TABLE", STRING);
          put("SOURCE_DATA_TYPE", SHORT);
          put("IS_AUTOINCREMENT", STRING);
          put("IS_GENERATEDCOLUMN", STRING);
        }
      });
  private final Map<String, ExprType> mapping;
}

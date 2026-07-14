/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class OutputLookupWriteExecTest {

  @SuppressWarnings("unchecked")
  @Test
  void inferMappingMapsSqlTypesToOpenSearchTypes() {
    RelDataTypeFactory tf = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType =
        tf.builder()
            .add("id", SqlTypeName.INTEGER)
            .add("count", SqlTypeName.BIGINT)
            .add("name", SqlTypeName.VARCHAR)
            .add("ts", SqlTypeName.TIMESTAMP)
            .add("ratio", SqlTypeName.DOUBLE)
            .add("ok", SqlTypeName.BOOLEAN)
            .add("_routing", SqlTypeName.VARCHAR)
            .build();

    Map<String, Object> mapping = OutputLookupWriteExec.inferMapping(rowType);
    Map<String, Object> props = (Map<String, Object>) mapping.get("properties");

    assertEquals("long", type(props, "id"));
    assertEquals("long", type(props, "count"));
    assertEquals("keyword", type(props, "name"));
    assertEquals("date", type(props, "ts"));
    assertEquals("double", type(props, "ratio"));
    assertEquals("boolean", type(props, "ok"));
    org.junit.jupiter.api.Assertions.assertFalse(
        props.containsKey("_routing"), "reserved metadata field must be excluded from the mapping");
  }

  @SuppressWarnings("unchecked")
  private static String type(Map<String, Object> props, String field) {
    return (String) ((Map<String, Object>) props.get(field)).get("type");
  }
}

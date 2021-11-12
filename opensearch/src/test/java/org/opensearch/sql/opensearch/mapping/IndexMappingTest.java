/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.mapping;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IndexMappingTest {

  @Test
  public void getFieldType() {
    IndexMapping indexMapping = new IndexMapping(ImmutableMap.of("name", "text"));
    assertEquals("text", indexMapping.getFieldType("name"));
    assertNull(indexMapping.getFieldType("not_exist"));
  }

  @Test
  public void getAllFieldTypes() {
    IndexMapping indexMapping = new IndexMapping(ImmutableMap.of("name", "text", "age", "int"));
    Map<String, String> fieldTypes = indexMapping.getAllFieldTypes(type -> "our_type");
    assertThat(
        fieldTypes,
        allOf(aMapWithSize(2), hasEntry("name", "our_type"), hasEntry("age", "our_type")));
  }
}

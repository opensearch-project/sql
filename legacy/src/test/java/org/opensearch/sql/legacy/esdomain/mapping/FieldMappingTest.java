/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.esdomain.mapping;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.util.MatcherUtils;

/**
 * Unit test for {@code FieldMapping} with trivial methods ignored such as isSpecified, isMetaField
 * etc.
 */
public class FieldMappingTest {

  @Test
  public void testFieldMatchesWildcardPatternSpecifiedInQuery() {
    assertThat(
        new FieldMapping("employee.first", emptyMap(), fieldsSpecifiedInQuery("employee.*")),
        isWildcardSpecified(true));
  }

  @Test
  public void testFieldMismatchesWildcardPatternSpecifiedInQuery() {
    assertThat(
        new FieldMapping("employee.first", emptyMap(), fieldsSpecifiedInQuery("manager.*")),
        isWildcardSpecified(false));
  }

  @Test
  public void testFieldIsProperty() {
    assertThat(new FieldMapping("employee.first"), isPropertyField(true));
  }

  @Test
  public void testNestedMultiFieldIsProperty() {
    assertThat(new FieldMapping("employee.first.keyword"), isPropertyField(true));
  }

  @Test
  public void testFieldIsNotProperty() {
    assertThat(new FieldMapping("employee"), isPropertyField(false));
  }

  @Test
  public void testMultiFieldIsNotProperty() {
    assertThat(new FieldMapping("employee.keyword"), isPropertyField(false));
  }

  @Test
  public void testUnknownFieldTreatedAsObject() {
    assertThat(new FieldMapping("employee"), hasType("object"));
  }

  @Test
  public void testDeepNestedField() {
    assertThat(
        new FieldMapping(
            "employee.location.city",
            ImmutableMap.of(
                "employee.location.city",
                new FieldMappingMetadata(
                    "employee.location.city",
                    new BytesArray(
                        "{\n" + "  \"city\" : {\n" + "    \"type\" : \"text\"\n" + "  }\n" + "}"))),
            emptyMap()),
        hasType("text"));
  }

  private Matcher<FieldMapping> isWildcardSpecified(boolean isMatched) {
    return MatcherUtils.featureValueOf(
        "is field match wildcard specified in query",
        is(isMatched),
        FieldMapping::isWildcardSpecified);
  }

  private Matcher<FieldMapping> isPropertyField(boolean isProperty) {
    return MatcherUtils.featureValueOf(
        "isPropertyField", is(isProperty), FieldMapping::isPropertyField);
  }

  private Matcher<FieldMapping> hasType(String expected) {
    return MatcherUtils.featureValueOf("type", is(expected), FieldMapping::type);
  }

  private Map<String, Field> fieldsSpecifiedInQuery(String... fieldNames) {
    return Arrays.stream(fieldNames)
        .collect(Collectors.toMap(name -> name, name -> new Field(name, "")));
  }
}

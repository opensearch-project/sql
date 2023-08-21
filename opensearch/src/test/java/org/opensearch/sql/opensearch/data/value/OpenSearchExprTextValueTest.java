/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchExprTextValueTest {
  @Test
  public void type_of_ExprTextValue() {
    assertEquals(OpenSearchTextType.of(), new OpenSearchExprTextValue("A").type());
  }

  @Test
  public void getFields() {
    var fields =
        Map.of(
            "f1", OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer),
            "f2", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
            "f3", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword));
    assertEquals(fields, OpenSearchTextType.of(fields).getFields());
  }

  @Test
  void text_type_without_fields_isnt_converted() {
    assertEquals(
        "field",
        OpenSearchDataType.of(OpenSearchDataType.MappingType.Text)
            .convertFieldForSearchQuery("field"));
  }

  @Test
  void text_type_with_fields_is_converted() {
    assertAll(
        () ->
            assertEquals(
                "field.keyword",
                OpenSearchTextType.of(
                        Map.of(
                            "keyword",
                            OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)))
                    .convertFieldForSearchQuery("field")),
        () ->
            assertEquals(
                "field.words",
                OpenSearchTextType.of(
                        Map.of(
                            "words", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)))
                    .convertFieldForSearchQuery("field")),
        () ->
            assertEquals(
                "field.numbers",
                OpenSearchTextType.of(
                        Map.of(
                            "numbers",
                            OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer)))
                    .convertFieldForSearchQuery("field")));
  }
}

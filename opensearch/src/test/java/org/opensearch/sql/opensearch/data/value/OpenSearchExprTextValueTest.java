/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

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
    var fields = Map.of(
        "f1", OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer),
        "f2", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword),
        "f3", OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword));
    assertEquals(fields, OpenSearchTextType.of(fields).getFields());
  }

  @Test
  void non_text_types_arent_converted() {
    assertAll(
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field",
            OpenSearchDataType.of(INTEGER))),
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field",
            OpenSearchDataType.of(STRING))),
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field",
            OpenSearchDataType.of(OpenSearchDataType.MappingType.GeoPoint))),
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field",
            OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword))),
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field",
            OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer))),
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field", STRING)),
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field", INTEGER))
    );
  }

  @Test
  void non_text_types_with_nested_objects_arent_converted() {
    var objectType = OpenSearchDataType.of(OpenSearchDataType.MappingType.Object,
        Map.of("subfield", OpenSearchDataType.of(STRING)));
    var arrayType = OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested,
        Map.of("subfield", OpenSearchDataType.of(STRING)));
    assertAll(
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field", objectType)),
        () -> assertEquals("field", OpenSearchTextType.convertTextToKeyword("field", arrayType))
    );
  }

  @Test
  void text_type_without_fields_isnt_converted() {
    assertEquals("field", OpenSearchTextType.convertTextToKeyword("field",
        OpenSearchDataType.of(OpenSearchDataType.MappingType.Text)));
  }

  @Test
  void text_type_with_fields_is_converted() {
    var textWithKeywordType = OpenSearchTextType.of(Map.of("keyword",
        OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword)));
    assertEquals("field.keyword",
        OpenSearchTextType.convertTextToKeyword("field", textWithKeywordType));
  }
}

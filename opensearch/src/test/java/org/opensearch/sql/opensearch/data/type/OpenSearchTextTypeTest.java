/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class OpenSearchTextTypeTest {

  @Test
  public void of() {
    assertSame(OpenSearchTextType.of(), OpenSearchTextType.of());
  }

  @Test
  public void fields_is_readonly() {
    assertThrows(Throwable.class, () -> OpenSearchTextType.of().getFields()
        .put("1", OpenSearchDataType.of(MappingType.Keyword)));
  }

  @Test
  public void of_map() {
    var fields = Map.of("1", OpenSearchDataType.of(MappingType.Keyword));
    assertAll(
        () -> assertNotSame(OpenSearchTextType.of(), OpenSearchTextType.of(Map.of())),
        () -> assertEquals(fields, OpenSearchTextType.of(fields).getFields())
    );
  }

  @Test
  public void expr_type() {
    assertAll(
        () -> assertEquals(STRING, OpenSearchTextType.of().getExprType()),
        () -> assertEquals(MappingType.Text, OpenSearchTextType.of().getMappingType())
    );
  }

  @Test
  public void cloneEmpty() {
    var fields = Map.of("1", OpenSearchDataType.of(MappingType.Keyword));
    var textType = OpenSearchTextType.of(fields);
    var clone = textType.cloneEmpty();
    assertAll(
        // `equals` falls back to `OpenSearchDataType`, which ignores `fields`
        () -> assertEquals(textType, clone),
        () -> assertTrue(clone instanceof OpenSearchTextType),
        () -> assertEquals(textType.getFields(), ((OpenSearchTextType) clone).getFields())
    );
  }

  @Test
  public void getParent() {
    assertEquals(STRING.getParent(), OpenSearchTextType.of().getParent());
  }

  @Test
  public void shouldCast() {
    assertAll(
        () -> assertFalse(OpenSearchTextType.of().shouldCast(STRING)),
        () -> assertTrue(OpenSearchTextType.of().shouldCast(DATE)),
        () -> assertTrue(OpenSearchTextType.of()
            .shouldCast(OpenSearchDataType.of(MappingType.Integer))),
        () -> assertFalse(OpenSearchTextType.of()
            .shouldCast(OpenSearchDataType.of(MappingType.Keyword)))
    );
  }

  @Test
  public void convertFieldForSearchQuery() {
    var fieldsWithKeyword = Map.of("words", OpenSearchDataType.of(MappingType.Keyword));
    var fieldsWithNumber = Map.of("numbers", OpenSearchDataType.of(MappingType.Integer));
    // use ImmutableMap to avoid entry reordering
    var fieldsWithMixed = ImmutableMap.of(
        "numbers", OpenSearchDataType.of(MappingType.Integer),
        "words", OpenSearchDataType.of(MappingType.Keyword));
    assertAll(
        () -> assertEquals("field", OpenSearchTextType.of().convertFieldForSearchQuery("field")),
        () -> assertEquals("field",
            OpenSearchTextType.of(Map.of()).convertFieldForSearchQuery("field")),
        () -> assertEquals("field.words",
            OpenSearchTextType.of(fieldsWithKeyword).convertFieldForSearchQuery("field")),
        () -> assertEquals("field.numbers",
            OpenSearchTextType.of(fieldsWithNumber).convertFieldForSearchQuery("field")),
        () -> assertEquals("field.words",
            OpenSearchTextType.of(fieldsWithMixed).convertFieldForSearchQuery("field"))
    );
  }
}

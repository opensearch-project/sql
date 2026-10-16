/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

/**
 * A document's value from map content, as a dedup row holds it, reads as a search hit reads it,
 * with or without field type tolerance: a field of several values gives them all only with it.
 */
class OpenSearchExprValueFactoryDocumentValueTest {

  private static OpenSearchExprValueFactory factory(boolean fieldTypeTolerance) {
    return new OpenSearchExprValueFactory(
        OpenSearchDataType.parseMapping(Map.of("tags", Map.of("type", "keyword"))),
        fieldTypeTolerance);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void documentValueReadsAsASearchHit(boolean fieldTypeTolerance) {
    OpenSearchExprValueFactory factory = factory(fieldTypeTolerance);
    for (List<String> tags : List.of(List.of("t1", "t2"), List.of("t1"))) {
      ExprValue fromHit =
          factory
              .construct("{\"tags\": [\"" + String.join("\", \"", tags) + "\"]}", false)
              .tupleValue()
              .get("tags");
      assertEquals(fromHit, factory.constructDocumentValue("tags", tags), tags::toString);
    }
    assertEquals(
        fieldTypeTolerance ? List.of("t1", "t2") : "t1",
        factory.constructDocumentValue("tags", List.of("t1", "t2")).value());
  }
}

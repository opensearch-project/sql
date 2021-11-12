/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LuceneQueryTest {

  @Test
  void should_not_support_single_argument_by_default() {
    DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());
    assertFalse(new LuceneQuery(){}.canSupport(dsl.abs(DSL.ref("age", INTEGER))));
  }

  @Test
  void should_throw_exception_if_not_implemented() {
    assertThrows(UnsupportedOperationException.class, () ->
        new LuceneQuery(){}.doBuild(null, null, null));
  }

}

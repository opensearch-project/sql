/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.scope;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.legacy.antlr.semantic.SemanticAnalysisException;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType;

public class TypeSupplierTest {
  @Rule public final ExpectedException exception = ExpectedException.none();

  @Test
  public void haveOneTypeShouldPass() {
    TypeSupplier age = new TypeSupplier("age", OpenSearchDataType.INTEGER);

    assertEquals(OpenSearchDataType.INTEGER, age.get());
  }

  @Test
  public void addSameTypeShouldPass() {
    TypeSupplier age = new TypeSupplier("age", OpenSearchDataType.INTEGER);
    age.add(OpenSearchDataType.INTEGER);

    assertEquals(OpenSearchDataType.INTEGER, age.get());
  }

  @Test
  public void haveTwoTypesShouldThrowException() {
    TypeSupplier age = new TypeSupplier("age", OpenSearchDataType.INTEGER);
    age.add(OpenSearchDataType.TEXT);

    exception.expect(SemanticAnalysisException.class);
    exception.expectMessage("Field [age] have conflict type");
    age.get();
  }
}

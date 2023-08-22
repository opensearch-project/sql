/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis.symbol;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprType;

public class SymbolTableTest {

  private SymbolTable symbolTable;

  @BeforeEach
  public void setup() {
    symbolTable = new SymbolTable();
  }

  @Test
  public void defineFieldSymbolShouldBeAbleToResolve() {
    defineSymbolShouldBeAbleToResolve(new Symbol(Namespace.FIELD_NAME, "age"), INTEGER);
  }

  @Test
  public void removeSymbolCannotBeResolve() {
    symbolTable.store(new Symbol(Namespace.FIELD_NAME, "age"), INTEGER);

    Optional<ExprType> age = symbolTable.lookup(new Symbol(Namespace.FIELD_NAME, "age"));
    assertTrue(age.isPresent());

    symbolTable.remove(new Symbol(Namespace.FIELD_NAME, "age"));
    age = symbolTable.lookup(new Symbol(Namespace.FIELD_NAME, "age"));
    assertFalse(age.isPresent());
  }

  @Test
  public void defineFieldSymbolShouldBeAbleToResolveByPrefix() {
    symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.projects.active"), BOOLEAN);
    symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.address"), STRING);
    symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.manager.name"), STRING);

    Map<String, ExprType> typeByName =
        symbolTable.lookupByPrefix(new Symbol(Namespace.FIELD_NAME, "s.projects"));

    assertThat(typeByName, allOf(aMapWithSize(1), hasEntry("s.projects.active", BOOLEAN)));
  }

  @Test
  public void lookupAllFieldsReturnUnnestedFields() {
    symbolTable.store(new Symbol(Namespace.FIELD_NAME, "active"), BOOLEAN);
    symbolTable.store(new Symbol(Namespace.FIELD_NAME, "active.manager"), STRING);
    symbolTable.store(new Symbol(Namespace.FIELD_NAME, "active.manager.name"), STRING);
    symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.address"), BOOLEAN);

    Map<String, ExprType> typeByName = symbolTable.lookupAllFields(Namespace.FIELD_NAME);

    assertThat(
        typeByName,
        allOf(aMapWithSize(2), hasEntry("active", BOOLEAN), hasEntry("s.address", BOOLEAN)));
  }

  @Test
  public void failedToResolveSymbolNoNamespaceMatched() {
    symbolTable.store(new Symbol(Namespace.FUNCTION_NAME, "customFunction"), BOOLEAN);
    assertFalse(symbolTable.lookup(new Symbol(Namespace.FIELD_NAME, "s.projects")).isPresent());

    assertThat(
        symbolTable.lookupByPrefix(new Symbol(Namespace.FIELD_NAME, "s.projects")), anEmptyMap());
  }

  @Test
  public void isEmpty() {
    symbolTable.store(new Symbol(Namespace.FUNCTION_NAME, "customFunction"), BOOLEAN);
    assertTrue(symbolTable.isEmpty(Namespace.FIELD_NAME));
  }

  private void defineSymbolShouldBeAbleToResolve(Symbol symbol, ExprType expectedType) {
    symbolTable.store(symbol, expectedType);

    Optional<ExprType> actualType = symbolTable.lookup(symbol);
    assertTrue(actualType.isPresent());
    assertEquals(expectedType, actualType.get());
  }
}

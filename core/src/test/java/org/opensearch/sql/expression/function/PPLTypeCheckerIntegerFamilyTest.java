/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link PPLTypeChecker#expectsIntegerFamilyAt}, which drives BIGINT-to-INTEGER narrowing
 * of int-domain function arguments (see {@code PPLFuncImpTable#narrowBigintArgs}).
 */
class PPLTypeCheckerIntegerFamilyTest {

  @Test
  void narrowsStrictIntegerPosition() {
    // e.g. ITEM(array, index) / LEFT(string, length): position 1 is strictly INTEGER.
    PPLTypeChecker checker = PPLTypeChecker.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER);
    assertFalse(checker.expectsIntegerFamilyAt(0)); // CHARACTER is not INTEGER
    assertTrue(checker.expectsIntegerFamilyAt(1)); // strict INTEGER
  }

  @Test
  void doesNotNarrowNumericValuePosition() {
    // NUMERIC maps to [INTEGER, DOUBLE]; a value position that also accepts DOUBLE must not be
    // narrowed (e.g. ROUND's first operand).
    PPLTypeChecker checker = PPLTypeChecker.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER);
    assertFalse(checker.expectsIntegerFamilyAt(0)); // NUMERIC -> not strictly INTEGER
    assertTrue(checker.expectsIntegerFamilyAt(1)); // strict INTEGER precision
  }

  @Test
  void doesNotNarrowNonNumericPosition() {
    PPLTypeChecker checker =
        PPLTypeChecker.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER);
    assertFalse(checker.expectsIntegerFamilyAt(0));
    assertFalse(checker.expectsIntegerFamilyAt(1));
  }

  @Test
  void doesNotNarrowOutOfRangePosition() {
    PPLTypeChecker checker = PPLTypeChecker.family(SqlTypeFamily.INTEGER);
    // No combination has a position 5 -> no evidence it needs INTEGER.
    assertFalse(checker.expectsIntegerFamilyAt(5));
  }

  @Test
  void compositeOrCheckerToleratesAnyInAlternateShape() {
    // ITEM: [ARRAY, INTEGER] OR [MAP, ANY]. Position 1 is INTEGER in the array shape and ANY in
    // the map shape; ANY (non-numeric alternate) is ignored, so the position still narrows.
    CompositeOperandTypeChecker composite =
        (CompositeOperandTypeChecker)
            OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER)
                .or(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.ANY));
    PPLTypeChecker checker = PPLTypeChecker.wrapComposite(composite, false);
    assertTrue(checker.expectsIntegerFamilyAt(1));
    assertFalse(checker.expectsIntegerFamilyAt(0)); // ARRAY / MAP, never INTEGER
  }

  @Test
  void compositeWithNumericAlternativeIsNotNarrowed() {
    // NUMERIC OR (NUMERIC, INTEGER): position 0 permits DOUBLE via NUMERIC, so never narrowed;
    // position 1 is strictly INTEGER.
    CompositeOperandTypeChecker composite =
        (CompositeOperandTypeChecker)
            OperandTypes.NUMERIC.or(
                OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER));
    PPLTypeChecker checker = PPLTypeChecker.wrapComposite(composite, false);
    assertFalse(checker.expectsIntegerFamilyAt(0));
    assertTrue(checker.expectsIntegerFamilyAt(1));
  }

  @Test
  void variadicIntegerPositionNarrows() {
    // ARRAY_SLICE(array, start, length): positions 1 and 2 are strictly INTEGER.
    PPLTypeChecker checker =
        PPLTypeChecker.family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER);
    assertFalse(checker.expectsIntegerFamilyAt(0));
    assertTrue(checker.expectsIntegerFamilyAt(1));
    assertTrue(checker.expectsIntegerFamilyAt(2));
  }

  @Test
  void sanityAllowedSignaturesNonEmpty() {
    // Guards against a regression where a family checker fails to enumerate parameter types.
    PPLTypeChecker checker = PPLTypeChecker.family(SqlTypeFamily.INTEGER);
    List<List<RelDataType>> params = checker.getParameterTypes();
    assertFalse(params.isEmpty());
  }
}

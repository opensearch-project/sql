/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.exception;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.knn.KnnErrors;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.VectorErrors;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.QueryProcessingStage;
import org.opensearch.sql.data.type.VectorDataType;

/** Errors of Calcite-only features never make a query fall back to the V2 engine. */
class SemanticCheckExceptionFallbackTest {

  @Test
  void unmarkedErrorsKeepTheirFallback() {
    SemanticCheckException plain = new SemanticCheckException("x");
    assertFalse(plain.isWithoutCalciteFallback());
    assertFalse(SemanticCheckException.preventsCalciteFallback(plain));
    assertFalse(
        SemanticCheckException.preventsCalciteFallback(new NonFallbackCalciteException("x")));
    assertFalse(SemanticCheckException.preventsCalciteFallback(new IllegalStateException("x")));
  }

  @Test
  void markedErrorIsFoundThroughWrappers() {
    SemanticCheckException marked = new SemanticCheckException("x").withoutCalciteFallback();
    assertSame(marked, marked.withoutCalciteFallback());
    assertTrue(SemanticCheckException.preventsCalciteFallback(marked));
    // Planning errors arrive wrapped in an ErrorReport, execution errors in runtime exceptions.
    ErrorReport report =
        ErrorReport.wrap(marked).stage(QueryProcessingStage.ANALYZING).location("here").build();
    assertTrue(SemanticCheckException.preventsCalciteFallback(report));
    assertTrue(
        SemanticCheckException.preventsCalciteFallback(
            new RuntimeException(new IllegalStateException(marked))));
  }

  @Test
  void vectorAndKnnErrorsAreMarked() {
    assertTrue(VectorErrors.vectorNotComparable().isWithoutCalciteFallback());
    assertTrue(VectorErrors.vectorNotComparable("detail").isWithoutCalciteFallback());
    assertTrue(
        VectorErrors.mixedVectorTypes(
                OpenSearchTypeFactory.TYPE_FACTORY.createVectorType(
                    2, VectorDataType.FLOAT, null, true),
                OpenSearchTypeFactory.TYPE_FACTORY.createVectorType(
                    3, VectorDataType.FLOAT, null, true))
            .isWithoutCalciteFallback());
    assertTrue(KnnErrors.missingK(null).isWithoutCalciteFallback());
    assertTrue(KnnErrors.kOutOfRange(0).isWithoutCalciteFallback());
    assertTrue(KnnErrors.knnPlacement().isWithoutCalciteFallback());
    assertTrue(KnnErrors.notPushedDown().isWithoutCalciteFallback());
  }
}

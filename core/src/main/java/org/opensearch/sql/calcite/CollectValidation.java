/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.Locale;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Shared plan-time refusal messages for the collect command destination. Both the foreground async
 * router in the transport layer and the collect operator in CalciteRelNodeVisitor produce identical
 * errors from these factories, so the refusal a caller sees does not depend on which layer fired
 * it.
 */
public final class CollectValidation {

  private CollectValidation() {}

  /** Refusal for a dot-prefixed (system or hidden) destination. */
  public static SemanticCheckException dotIndexRefused(String indexName) {
    return new SemanticCheckException(
        String.format(
            Locale.ROOT,
            "collect cannot write to system or hidden index [%s]; dot-prefixed indices are refused",
            indexName));
  }

  /** Refusal for a destination index that does not exist. */
  public static SemanticCheckException destinationDoesNotExist(String indexName) {
    return new SemanticCheckException(
        String.format(
            Locale.ROOT,
            "collect destination index [%s] does not exist; collect requires a pre-existing"
                + " destination index",
            indexName));
  }
}

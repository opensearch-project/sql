/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import lombok.SneakyThrows;
import org.opensearch.sql.ppl.WhereCommandIT;

public class CalciteWhereCommandIT extends WhereCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void testIsNotNullFunction() throws IOException {
    withFallbackEnabled(
        () -> {
          try {
            super.testIsNotNullFunction();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3428");
  }

  @SneakyThrows
  @Override
  public void testWhereWithMetadataFields() {
    withFallbackEnabled(
        () -> {
          try {
            super.testWhereWithMetadataFields();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-poject/sql/issues/3333");
  }

  @Override
  protected String getIncompatibleTypeErrMsg() {
    return "In expression types are incompatible: fields type LONG, values type [INTEGER, INTEGER,"
        + " STRING]";
  }
}

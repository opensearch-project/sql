/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.opensearch.sql.ppl.FieldsCommandIT;

public class CalciteFieldsCommandIT extends FieldsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void testDelimitedMetadataFields() throws IOException {
    withFallbackEnabled(
        () -> {
          try {
            super.testDelimitedMetadataFields();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "Calcite doesn't support metadata fields in fields yet");
  }

  @Override
  public void testMetadataFields() throws IOException {
    withFallbackEnabled(
        () -> {
          try {
            super.testMetadataFields();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "Calcite doesn't support metadata fields in fields yet");
  }
}

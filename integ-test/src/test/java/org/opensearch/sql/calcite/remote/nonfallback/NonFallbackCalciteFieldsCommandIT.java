/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.FieldsCommandIT;

public class NonFallbackCalciteFieldsCommandIT extends FieldsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  @Test
  @Ignore("Calcite doesn't support metadata fields in fields yet")
  public void testDelimitedMetadataFields() throws IOException {
    super.testDelimitedMetadataFields();
  }

  @Override
  @Test
  @Ignore("Calcite doesn't support metadata fields in fields yet")
  public void testMetadataFields() throws IOException {
    super.testMetadataFields();
  }
}

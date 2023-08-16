/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.storage;

import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StorageEngineTest {

  @Test
  void testFunctionsMethod() {
    StorageEngine k = (dataSourceSchemaName, tableName) -> null;
    Assertions.assertEquals(Collections.emptyList(), k.getFunctions());
  }
}

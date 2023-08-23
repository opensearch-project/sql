/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.querybuilders;

import java.util.Collections;
import java.util.Date;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.prometheus.storage.querybuilder.StepParameterResolver;

public class StepParameterResolverTest {

  @Test
  void testNullChecks() {
    StepParameterResolver stepParameterResolver = new StepParameterResolver();
    Assertions.assertThrows(
        NullPointerException.class,
        () -> stepParameterResolver.resolve(null, new Date().getTime(), Collections.emptyList()));
    Assertions.assertThrows(
        NullPointerException.class,
        () -> stepParameterResolver.resolve(new Date().getTime(), null, Collections.emptyList()));
  }
}

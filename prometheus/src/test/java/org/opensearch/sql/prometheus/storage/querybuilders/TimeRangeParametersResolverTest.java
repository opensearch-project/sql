/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.querybuilders;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.apache.commons.math3.util.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.prometheus.storage.querybuilder.TimeRangeParametersResolver;

public class TimeRangeParametersResolverTest {

  @Test
  void testTimeRangeParametersWithoutTimestampFilter() {
    TimeRangeParametersResolver timeRangeParametersResolver = new TimeRangeParametersResolver();
    Pair<Long, Long> result = timeRangeParametersResolver.resolve(
        DSL.and(DSL.less(DSL.ref("code", STRING), DSL.literal(stringValue("200"))),
            DSL.equal(DSL.ref("handler", STRING), DSL.literal(stringValue("/ready/")))));
    Assertions.assertNotNull(result);
    Assertions.assertEquals(3600, result.getSecond() - result.getFirst());
  }
}

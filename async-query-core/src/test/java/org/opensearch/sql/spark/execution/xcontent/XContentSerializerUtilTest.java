package org.opensearch.sql.spark.execution.xcontent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class XContentSerializerUtilTest {
  @Test
  public void testBuildMetadata() {
    ImmutableMap<String, Object> result = XContentSerializerUtil.buildMetadata(1, 2);

    assertEquals(2, result.size());
    assertEquals(1L, result.get(XContentSerializerUtil.SEQ_NO));
    assertEquals(2L, result.get(XContentSerializerUtil.PRIMARY_TERM));
  }
}

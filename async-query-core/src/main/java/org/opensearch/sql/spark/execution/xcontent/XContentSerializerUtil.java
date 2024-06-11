package org.opensearch.sql.spark.execution.xcontent;

import com.google.common.collect.ImmutableMap;
import lombok.experimental.UtilityClass;

@UtilityClass
public class XContentSerializerUtil {
  public static final String SEQ_NO = "seqNo";
  public static final String PRIMARY_TERM = "primaryTerm";

  public static ImmutableMap<String, Object> buildMetadata(long seqNo, long primaryTerm) {
    return ImmutableMap.of(SEQ_NO, seqNo, PRIMARY_TERM, primaryTerm);
  }
}

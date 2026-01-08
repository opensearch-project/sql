/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

@UtilityClass
public class GraphLookupUtils {
  public static final String GRAPH_LOOKUP_PREFIX = "gl_";
  public static final String SRC_FIELD_SUFFIX = GRAPH_LOOKUP_PREFIX + "src_";
  public static final String HIER_FIELD_SUFFIX = GRAPH_LOOKUP_PREFIX + "hier_";
  public static final String ANCHOR_FROM_ALIAS = GRAPH_LOOKUP_PREFIX + "anchor_from";
  public static final String ANCHOR_TO_ALIAS = GRAPH_LOOKUP_PREFIX + "anchor_to";
  public static final String DEPTH_FIELD = GRAPH_LOOKUP_PREFIX + "depth";
  public static final String RECURSIVE_FROM_ALIAS = GRAPH_LOOKUP_PREFIX + "rec_from";
  public static final String RECURSIVE_TO_ALIAS = GRAPH_LOOKUP_PREFIX + "rec_to";
  public static final String RECURSIVE_TABLE_NAME = GRAPH_LOOKUP_PREFIX + "recursive";

  public List<String> createAliases(List<String> fields) {
    List<String> aliases = new ArrayList<>();
    for (String field : fields) {
      aliases.add(SRC_FIELD_SUFFIX + field);
    }
    for (String field : fields) {
      aliases.add(HIER_FIELD_SUFFIX + field);
    }
    aliases.add(DEPTH_FIELD);
    return aliases;
  }

  public List<RexNode> createAnchorProjections(RelBuilder builder, List<String> fields) {
    List<RexNode> projections = new ArrayList<>();
    for (String field : fields) {
      projections.add(builder.field(ANCHOR_FROM_ALIAS, field));
    }
    for (String field : fields) {
      projections.add(builder.field(ANCHOR_TO_ALIAS, field));
    }
    projections.add(builder.literal(1));
    return projections;
  }

  public List<RexNode> createRecursiveProjections(RelBuilder builder, List<String> fields) {
    List<RexNode> projections = new ArrayList<>();
    for (String field : fields) {
      projections.add(builder.field(RECURSIVE_FROM_ALIAS, SRC_FIELD_SUFFIX + field));
    }
    for (String field : fields) {
      projections.add(builder.field(RECURSIVE_TO_ALIAS, field));
    }
    projections.add(
        builder.call(
            SqlStdOperatorTable.PLUS,
            builder.field(RECURSIVE_FROM_ALIAS, DEPTH_FIELD),
            builder.literal(1)));
    return projections;
  }
}

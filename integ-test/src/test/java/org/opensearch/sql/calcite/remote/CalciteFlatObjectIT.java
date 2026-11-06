/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_FLAT_OBJECT;
import static org.opensearch.sql.util.Capability.MULTI_VALUE_FIELD_LOAD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

@RequiresCapability(
    value = MULTI_VALUE_FIELD_LOAD,
    note =
        "reads the flat_object dataset, whose multi-value field can't load on the AE store"
            + " (MULTI_VALUE_FIELD_LOAD).")
public class CalciteFlatObjectIT extends PPLIntegTestCase {

  private static final String MAX_DEPTH = "plugins.query.flat_object.max_depth";

  /** The leaf of the "deep" record, written 25 levels down: attributes.l1. ... .l24.leaf. */
  private static final String DEEP_LEAF =
      "attributes."
          + IntStream.rangeClosed(1, 24).mapToObj(i -> "l" + i).collect(Collectors.joining("."))
          + ".leaf";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.FLAT_OBJECT);
  }

  // Past the default depth of 20 the rest of the object is kept as one leaf, so the deep leaf is
  // not found under its own path -- by the engine's read, or by a filter, pushed down as a script
  // or evaluated after reading.
  @Test
  public void testLeafPastTheDefaultDepthIsNotReadUnderItsPath() throws IOException {
    verifyDataRows(readDeepLeaf(), rows((Object) null));
    verifyNumOfRows(filterOnDeepLeaf(), 0);
  }

  // The setting moves the cap on every node, so the engine's read and a script on the data node
  // both reach the leaf once it is raised past the document's depth.
  @Test
  public void testRaisingTheMaxDepthReadsTheLeaf() throws IOException {
    try {
      updateClusterSettings(new ClusterSetting(TRANSIENT, MAX_DEPTH, "30"));
      verifyDataRows(readDeepLeaf(), rows("x"));
      verifyDataRows(filterOnDeepLeaf(), rows("deep"));
    } finally {
      updateClusterSettings(new ClusterSetting(TRANSIENT, MAX_DEPTH, null));
    }
  }

  private JSONObject readDeepLeaf() throws IOException {
    return executeQuery(
        String.format(
            "source=%s | where service = 'deep' | fields %s", TEST_INDEX_FLAT_OBJECT, DEEP_LEAF));
  }

  /** A function of the leaf is no term lookup, so with pushdown it runs as a script. */
  private JSONObject filterOnDeepLeaf() throws IOException {
    return executeQuery(
        String.format(
            "source=%s | where length(%s) = 1 | fields service",
            TEST_INDEX_FLAT_OBJECT, DEEP_LEAF));
  }
}

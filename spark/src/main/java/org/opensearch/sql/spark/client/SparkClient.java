/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import java.io.IOException;
import org.json.JSONObject;

/**
 * Interface class for Spark Client.
 */
public interface SparkClient {
  /**
   * @param query spark sql query
   * @return      spark query response
   * @throws      IOException
   */
  JSONObject sql(String query) throws IOException;
}
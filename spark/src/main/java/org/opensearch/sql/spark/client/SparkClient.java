/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import java.io.IOException;
import org.json.JSONObject;

public interface SparkClient {
  JSONObject sql(String query) throws IOException;
}
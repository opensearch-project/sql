/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import org.json.JSONObject;

import java.io.IOException;

public interface SparkClient {

    JSONObject sql(String query) throws IOException;
}
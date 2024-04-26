/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_REQUEST_BUFFER_INDEX_NAME;

import java.util.Locale;
import lombok.experimental.UtilityClass;

@UtilityClass
public class OpenSearchStateStoreUtil {

  public static String getIndexName(String datasourceName) {
    return String.format(
        "%s_%s", SPARK_REQUEST_BUFFER_INDEX_NAME, datasourceName.toLowerCase(Locale.ROOT));
  }
}

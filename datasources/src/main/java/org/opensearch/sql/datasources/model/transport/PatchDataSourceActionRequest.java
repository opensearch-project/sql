/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.model.transport;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.CONNECTOR_FIELD;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.NAME_FIELD;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.PROPERTIES_FIELD;

import java.io.IOException;
import java.util.Map;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;

public class PatchDataSourceActionRequest extends ActionRequest {

  @Getter private Map<String, Object> dataSourceData;

  /** Constructor of UpdateDataSourceActionRequest from StreamInput. */
  public PatchDataSourceActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public PatchDataSourceActionRequest(Map<String, Object> dataSourceData) {
    this.dataSourceData = dataSourceData;
  }

  @Override
  public ActionRequestValidationException validate() {
    if (this.dataSourceData.get(NAME_FIELD).equals(DEFAULT_DATASOURCE_NAME)) {
      ActionRequestValidationException exception = new ActionRequestValidationException();
      exception.addValidationError(
          "Not allowed to update datasource with name : " + DEFAULT_DATASOURCE_NAME);
      return exception;
    } else if (this.dataSourceData.get(CONNECTOR_FIELD) != null) {
      ActionRequestValidationException exception = new ActionRequestValidationException();
      exception.addValidationError("Not allowed to update connector for datasource");
      return exception;
    } else if (((Map<String, String>) this.dataSourceData.get(PROPERTIES_FIELD))
        .keySet().stream().anyMatch(key -> key.endsWith("role_arn"))) {
      ActionRequestValidationException exception = new ActionRequestValidationException();
      exception.addValidationError("Not allowed to update role_arn");
      return exception;
    } else {
      return null;
    }
  }
}

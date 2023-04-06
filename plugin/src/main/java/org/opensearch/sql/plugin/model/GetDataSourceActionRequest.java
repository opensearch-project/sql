/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.model;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import java.io.IOException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

@NoArgsConstructor
public class GetDataSourceActionRequest extends ActionRequest {

  @Getter
  private String dataSourceName;

  /**
   * Constructor of GetDataSourceActionRequest from StreamInput.
   */
  public GetDataSourceActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public GetDataSourceActionRequest(String dataSourceName) {
    this.dataSourceName = dataSourceName;
  }

  @Override
  public ActionRequestValidationException validate() {
    if (this.dataSourceName != null && this.dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
      ActionRequestValidationException exception = new ActionRequestValidationException();
      exception
          .addValidationError(
              "Not allowed to fetch datasource with name : " + DEFAULT_DATASOURCE_NAME);
      return exception;
    } else {
      return null;
    }
  }

}
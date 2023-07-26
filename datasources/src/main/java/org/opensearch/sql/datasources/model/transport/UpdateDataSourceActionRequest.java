/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.model.transport;


import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

public class UpdateDataSourceActionRequest
    extends ActionRequest {

  @Getter
  private DataSourceMetadata dataSourceMetadata;

  /** Constructor of UpdateDataSourceActionRequest from StreamInput. */
  public UpdateDataSourceActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public UpdateDataSourceActionRequest(DataSourceMetadata dataSourceMetadata) {
    this.dataSourceMetadata = dataSourceMetadata;
  }

  @Override
  public ActionRequestValidationException validate() {
    if (this.dataSourceMetadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      ActionRequestValidationException exception = new ActionRequestValidationException();
      exception
          .addValidationError(
              "Not allowed to update datasource with name : " + DEFAULT_DATASOURCE_NAME);
      return exception;
    } else {
      return null;
    }
  }
}

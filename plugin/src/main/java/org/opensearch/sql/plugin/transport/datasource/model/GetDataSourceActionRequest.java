/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.transport.datasource.model;

import java.io.IOException;
import lombok.NoArgsConstructor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

@NoArgsConstructor
public class GetDataSourceActionRequest extends ActionRequest {

  /** Constructor of GetDataSourceActionRequest from StreamInput. */
  public GetDataSourceActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

}

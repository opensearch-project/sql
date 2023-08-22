/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import org.json.JSONObject;
import org.opensearch.core.rest.RestStatus;

public class ErrorMessage<E extends Exception> {

  protected E exception;

  private int status;
  private String type;
  private String reason;
  private String details;

  public ErrorMessage(E exception, int status) {
    this.exception = exception;
    this.status = status;

    this.type = fetchType();
    this.reason = fetchReason();
    this.details = fetchDetails();
  }

  private String fetchType() {
    return exception.getClass().getSimpleName();
  }

  protected String fetchReason() {
    return status == RestStatus.BAD_REQUEST.getStatus()
        ? "Invalid SQL query"
        : "There was internal problem at backend";
  }

  protected String fetchDetails() {
    // Some exception prints internal information (full class name) which is security concern
    // return exception.toString();
    return emptyStringIfNull(exception.getLocalizedMessage());
  }

  private String emptyStringIfNull(String str) {
    return str != null ? str : "";
  }

  @Override
  public String toString() {
    JSONObject output = new JSONObject();

    output.put("status", status);
    output.put("error", getErrorAsJson());

    return output.toString(2);
  }

  private JSONObject getErrorAsJson() {
    JSONObject errorJson = new JSONObject();

    errorJson.put("type", type);
    errorJson.put("reason", reason);
    errorJson.put("details", details);

    return errorJson;
  }
}

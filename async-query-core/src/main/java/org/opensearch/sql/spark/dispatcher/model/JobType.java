/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

public enum JobType {
  INTERACTIVE("interactive"),
  STREAMING("streaming"),
  REFRESH("refresh"),
  BATCH("batch");

  private String text;

  JobType(String text) {
    this.text = text;
  }

  public String getText() {
    return this.text;
  }

  /**
   * Get JobType from text.
   *
   * @param text text.
   * @return JobType {@link JobType}.
   */
  public static JobType fromString(String text) {
    for (JobType JobType : JobType.values()) {
      if (JobType.text.equalsIgnoreCase(text)) {
        return JobType;
      }
    }
    throw new IllegalArgumentException("No JobType with text " + text + " found");
  }
}

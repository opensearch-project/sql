/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.storage;

public class OSS3Object {
  private final String bucket;

  private final String object;

  public OSS3Object(String bucket, String object) {
    this.bucket = bucket;
    this.object = object;
  }

  @Override
  public String toString() {
    return "OSS3Object{" + "bucket='" + bucket + '\'' + ", object='" + object + '\'' + '}';
  }

  public String getBucket() {
    return bucket;
  }

  public String getObject() {
    return object;
  }
}

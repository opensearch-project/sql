/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.storage;

import java.time.Instant;
import java.util.Objects;
import lombok.EqualsAndHashCode;

public class OSS3Object {
  private final String bucket;

  private final String object;

  private Instant lastModified;

  public OSS3Object(String bucket, String object) {
    this.bucket = bucket;
    this.object = object;
    this.lastModified = Instant.now();
  }

  public OSS3Object(String bucket, String object, Instant lastModified) {
    this.bucket = bucket;
    this.object = object;
    this.lastModified = lastModified;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OSS3Object that = (OSS3Object) o;
    return Objects.equals(bucket, that.bucket) &&
        Objects.equals(object, that.object);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, object);
  }
}

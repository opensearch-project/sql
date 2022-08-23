/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.s3.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3Reader implements Iterator<String> {
  private static final Logger log = LogManager.getLogger(S3Reader.class);

  /** S3Client. */
  private final S3Client s3;

  /** Current Iterator. */
  private Iterator<String> currIterator;

  private BufferedReader reader;

  public S3Reader() {
    s3 =
        doPrivileged(
            () ->
                S3Client.builder()
                    .region(Region.US_WEST_2)
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build());
  }

  public void open(OSS3Object s3Object) {
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(s3Object.getBucket()).key(s3Object.getObject()).build();
    ResponseBytes<GetObjectResponse> s3Objects =
        doPrivileged(() -> s3.getObjectAsBytes(getObjectRequest));
    try {
      reader =
          new BufferedReader(new InputStreamReader(new GZIPInputStream(s3Objects.asInputStream())));
      currIterator = reader.lines().iterator();
    } catch (Exception e) {
      log.error("failed to read s3 object {} ", s3Object, e);
      throw new RuntimeException(e);
    }
  }

  public void close() {
    currIterator = null;
    try {
      reader.close();
    } catch (IOException e) {
      log.error("failed to close read stream", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return currIterator != null && currIterator.hasNext();
  }

  @Override
  public String next() {
    return currIterator.next();
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }
}

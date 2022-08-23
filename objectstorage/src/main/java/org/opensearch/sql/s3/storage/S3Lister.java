/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.storage;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3Lister {

  private static final Logger log = LogManager.getLogger(S3Lister.class);

  /** S3Client. */
  private final S3Client s3;

  private final String bucket;

  private final String prefix;

  public S3Lister(URI uri) {
    this.bucket = uri.getHost();
    this.prefix = uri.getPath().substring(1);
    this.s3 =
        doPrivileged(
            () ->
                S3Client.builder()
                    .region(Region.US_WEST_2)
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build());
  }

  public Iterable<List<OSS3Object>> partition(int N) {
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();

    Iterable<List<OSS3Object>> res =
        doPrivileged(
            () -> {
              ListObjectsV2Iterable responseIterable = s3.listObjectsV2Paginator(request);
              List<OSS3Object> objects = new ArrayList<>();

              for (ListObjectsV2Response response : responseIterable) {
                for (S3Object content : response.contents()) {
                  log.info("s3 object {}", content.key());
                  objects.add(new OSS3Object(bucket, content.key()));
                }
              }

              int partitionSize = Math.max(1, objects.size() / N);
              return Iterables.partition(objects, partitionSize);
            });
    return res;
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }
}

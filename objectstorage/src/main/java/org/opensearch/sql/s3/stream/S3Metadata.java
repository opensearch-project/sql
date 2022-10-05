/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.stream;

import java.util.Set;
import lombok.Data;
import org.opensearch.sql.s3.storage.OSS3Object;

@Data
public class S3Metadata {

  private final Set<OSS3Object> paths;

  private final Long batchId;
}

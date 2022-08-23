/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.s3.storage;

import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.storage.TableScanOperator;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class S3ScanOperator extends TableScanOperator {

  private static final Logger log = LogManager.getLogger(S3ScanOperator.class);

  private static int PARTITION = 1;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Iterator<OSS3Object> partitions;

  private final S3Reader s3Reader;

  private final String tableName;

  public S3ScanOperator(String tableName) {
    this.tableName = tableName;
    this.s3Reader = new S3Reader();
  }

  @Override
  public String explain() {
    return "S3ScanOperator";
  }

  @Override
  public boolean hasNext() {
    if (s3Reader.hasNext()) {
      return true;
    } else if (!partitions.hasNext()) {
      return false;
    } else {
      s3Reader.close();
      final OSS3Object next = partitions.next();
      log.info("next file {}", next);
      s3Reader.open(next);
      return s3Reader.hasNext();
    }
  }

  @Override
  public ExprValue next() {
    TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {};
    try {
      return tupleValue(OBJECT_MAPPER.readValue(s3Reader.next(), typeRef));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("S3ScanOperator exception", e);
    }
  }

  @Override
  public void open() {
    try {
      S3Lister s3Lister = new S3Lister(new URI(tableName));
      Iterable<List<OSS3Object>> partition = s3Lister.partition(PARTITION);
      List<OSS3Object> result = new ArrayList<>();
      for (List<OSS3Object> oss3Objects : partition) {
        result.addAll(oss3Objects);
      }
      this.partitions = result.iterator();
      OSS3Object next = this.partitions.next();

      log.info("next file {}", next);
      s3Reader.open(next);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    s3Reader.close();
  }
}

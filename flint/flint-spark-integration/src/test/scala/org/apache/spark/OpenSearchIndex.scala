/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.xcontent.XContentType

/**
 * Provide OpenSearch Index
 */
trait OpenSearchIndex { self: OpenSearchSuite =>

  val oneNodeSetting = """{
                         |  "number_of_shards": "1",
                         |  "number_of_replicas": "0"
                         |}""".stripMargin

  def simpleIndex(indexName: String): Unit = {
    val mappings = """{
                     |  "properties": {
                     |    "accountId": {
                     |      "type": "keyword"
                     |    },
                     |    "eventName": {
                     |      "type": "keyword"
                     |    },
                     |    "eventSource": {
                     |      "type": "keyword"
                     |    }
                     |  }
                     |}""".stripMargin
    val docs = Seq("""{
                     |  "accountId": "123",
                     |  "eventName": "event",
                     |  "eventSource": "source"
                     |}""".stripMargin)
    index(indexName, oneNodeSetting, mappings, docs)
  }

  def index(index: String, settings: String, mappings: String, doc: Seq[String]): Unit = {
    openSearchClient.indices.create(
      new CreateIndexRequest(index)
        .settings(settings, XContentType.JSON)
        .mapping(mappings, XContentType.JSON),
      RequestOptions.DEFAULT)

    val indexRequest = doc.foldLeft(new IndexRequest(index))((req, source) =>
      req.source(source, XContentType.JSON))
    val response =
      openSearchClient.bulk(new BulkRequest().add(indexRequest), RequestOptions.DEFAULT)

    assume(
      !response.hasFailures,
      s"bulk index docs to $index failed: ${response.buildFailureMessage()}")
  }
}

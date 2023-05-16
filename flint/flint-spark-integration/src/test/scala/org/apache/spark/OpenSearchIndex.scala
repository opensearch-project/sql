/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest}
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

  def multipleDocIndex(indexName: String, N: Int): Unit = {
    val mappings = """{
                     |  "properties": {
                     |    "id": {
                     |      "type": "integer"
                     |    }
                     |  }
                     |}""".stripMargin

    val docs = for (n <- 1 to N) yield s"""{"id": $n}""".stripMargin
    index(indexName, oneNodeSetting, mappings, docs)
  }

  def index(index: String, settings: String, mappings: String, docs: Seq[String]): Unit = {
    openSearchClient.indices.create(
      new CreateIndexRequest(index)
        .settings(settings, XContentType.JSON)
        .mapping(mappings, XContentType.JSON),
      RequestOptions.DEFAULT)

    val getIndexResponse =
      openSearchClient.indices().get(new GetIndexRequest(index), RequestOptions.DEFAULT)
    assume(getIndexResponse.getIndices.contains(index), s"create index $index failed")

    /**
     *   1. Wait until refresh the index.
     */
    val request = new BulkRequest().setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
    for (doc <- docs) {
      request.add(new IndexRequest(index).source(doc, XContentType.JSON))
    }
    val response =
      openSearchClient.bulk(request, RequestOptions.DEFAULT)

    assume(
      !response.hasFailures,
      s"bulk index docs to $index failed: ${response.buildFailureMessage()}")
  }
}

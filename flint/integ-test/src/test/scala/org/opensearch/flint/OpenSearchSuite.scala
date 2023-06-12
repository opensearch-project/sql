/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint

import org.apache.http.HttpHost
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.opensearch.common.xcontent.XContentType
import org.opensearch.testcontainers.OpenSearchContainer
import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.sql.flint.config.FlintSparkConf.{HOST_ENDPOINT, HOST_PORT, IGNORE_DOC_ID_COLUMN, REFRESH_POLICY}

/**
 * Test required OpenSearch domain should extend OpenSearchSuite.
 */
trait OpenSearchSuite extends BeforeAndAfterAll {
  self: Suite =>

  protected lazy val container = new OpenSearchContainer()

  protected lazy val openSearchPort: Int = container.port()

  protected lazy val openSearchHost: String = container.getHost

  protected lazy val openSearchClient = new RestHighLevelClient(
    RestClient.builder(new HttpHost(openSearchHost, openSearchPort, "http")))

  protected lazy val openSearchOptions =
    Map(
      s"${HOST_ENDPOINT.optionKey}" -> openSearchHost,
      s"${HOST_PORT.optionKey}" -> s"$openSearchPort",
      s"${REFRESH_POLICY.optionKey}" -> "wait_for",
      s"${IGNORE_DOC_ID_COLUMN.optionKey}" -> "false")

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    container.close()
    super.afterAll()
  }

  /**
   * Delete index `indexNames` after calling `f`.
   */
  protected def withIndexName(indexNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      indexNames.foreach { indexName =>
        openSearchClient
          .indices()
          .delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT)
      }
    }
  }

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
    if (docs.nonEmpty) {
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
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.apache.http.HttpHost
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.opensearch.testcontainers.OpenSearchContainer
import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.util.Utils

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
    Map("host" -> openSearchHost, "port" -> s"$openSearchPort")

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
    Utils.tryWithSafeFinally(f) {
      indexNames.foreach { indexName =>
        openSearchClient
          .indices()
          .delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT)
      }
    }
  }
}

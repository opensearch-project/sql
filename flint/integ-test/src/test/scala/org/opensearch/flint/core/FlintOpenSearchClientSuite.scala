/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import scala.collection.JavaConverters._

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintOpenSearchClientSuite extends AnyFlatSpec with OpenSearchSuite with Matchers {

  /** Lazy initialize after container started. */
  lazy val flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))

  behavior of "Flint OpenSearch client"

  it should "create index successfully" in {
    val indexName = "test"
    val content =
      """ {
        |   "_meta": {
        |     "kind": "SkippingIndex"
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin
    flintClient.createIndex(indexName, new FlintMetadata(content))

    flintClient.exists(indexName) shouldBe true
    flintClient.getIndexMetadata(indexName).getContent should matchJson(content)
  }

  it should "return false if index not exist" in {
    flintClient.exists("non-exist-index") shouldBe false
  }

  it should "read docs from index as string successfully " in {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val match_all = null
      val reader = flintClient.createReader(indexName, match_all)

      reader.hasNext shouldBe true
      reader.next shouldBe """{"accountId":"123","eventName":"event","eventSource":"source"}"""
      reader.hasNext shouldBe false
      reader.close()
    }
  }
}

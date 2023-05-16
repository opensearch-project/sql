/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class FlintOpenSearchClientSuite
    extends AnyFlatSpec
    with OpenSearchSuite
    with Matchers {

  /** Lazy initialize after container started. */
  lazy val flintClient = new FlintOpenSearchClient(openSearchHost, openSearchPort)

  behavior of "Flint OpenSearch client"

  it should "create index successfully" in {
    val indexName = "test"
    val schema = Map("age" -> "integer").asJava
    val meta =
      Map("index" ->
        Map("kind" -> "SkippingIndex").asJava.asInstanceOf[Object]
      ).asJava
    flintClient.createIndex(indexName, new FlintMetadata(schema, meta))

    flintClient.exists(indexName) shouldBe true
    flintClient.getIndexMetadata(indexName) should have (
      'schema (schema),
      'meta (meta))
  }

  it should "return false if index not exist" in {
    flintClient.exists("non-exist-index") shouldBe false
  }
}

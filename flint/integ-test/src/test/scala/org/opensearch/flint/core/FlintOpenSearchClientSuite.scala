/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.collection.JavaConverters._

class FlintOpenSearchClientSuite
    extends AnyFlatSpec
    with OpenSearchSuite {

  lazy val flintClient = new FlintOpenSearchClient(openSearchHost, openSearchPort)

  it should "create index successfully" in {
    val schema = Map("age" -> "integer").asJava
    val meta =
      Map("index" ->
        Map("kind" -> "SkippingIndex").asJava.asInstanceOf[Object]
      ).asJava
    flintClient.createIndex("test", new FlintMetadata(schema, meta))

    val metadata = flintClient.getIndexMetadata("test")
    metadata.getSchema shouldBe schema
    metadata.getMeta shouldBe meta
  }
}

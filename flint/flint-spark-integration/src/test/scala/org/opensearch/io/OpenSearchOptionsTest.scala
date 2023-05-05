/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.io

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkFunSuite

class OpenSearchOptionsTest extends SparkFunSuite with Matchers with BeforeAndAfter {
  var options: mutable.Map[String, String] = _
  var openSearchOptions: OpenSearchOptions = _

  before {
    options = mutable.Map(
      OpenSearchOptions.INDEX_NAME -> "myindex",
      OpenSearchOptions.HOST -> "localhost",
      OpenSearchOptions.PORT -> "9200")
    openSearchOptions = new OpenSearchOptions(options.asJava)
  }

  test("OpenSearchOptions should return the correct index name") {
    assert(openSearchOptions.getIndexName == "myindex")
  }

  test("OpenSearchOptions should return the correct host") {
    assert(openSearchOptions.getHost == "localhost")
  }

  test("OpenSearchOptions should return the correct port") {
    assert(openSearchOptions.getPort == 9200)
  }

  test("OpenSearchOptions should throw a NumberFormatException when port number is invalid") {
    options.put(OpenSearchOptions.PORT, "invalid-port-number")
    openSearchOptions = new OpenSearchOptions(options.asJava)
    assertThrows[NumberFormatException] {
      openSearchOptions.getPort
    }
  }

  test("OpenSearchOptions should return the mocked port number") {
    val mockedOptions = mock[mutable.Map[String, String]]
    when(mockedOptions.get(OpenSearchOptions.PORT)).thenReturn(Some("9300"))
    openSearchOptions = new OpenSearchOptions(mockedOptions.asJava)
    assert(openSearchOptions.getPort == 9300)
  }
}

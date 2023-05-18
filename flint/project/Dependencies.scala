/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import sbt._

object Dependencies {
  def deps(sparkVersion: String): Seq[ModuleID] = {
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources (),
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources (),
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests")
  }
}

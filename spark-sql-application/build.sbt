/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

name := "sql-job"

version := "1.0"

scalaVersion := "2.12.15"

val sparkVersion = "3.3.2"

mainClass := Some("org.opensearch.sql.SQLJob")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "sql-job.jar"
}

resolvers ++= Seq(
  ("apache-snapshots" at "http://repository.apache.org/snapshots/").withAllowInsecureProtocol(true)
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

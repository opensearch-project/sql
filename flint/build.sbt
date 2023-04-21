/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

lazy val scala212 = "2.12.14"
lazy val sparkVersion = "3.3.1"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := scala212

lazy val root = (project in file("."))
  .aggregate(flintCore, flintSparkIntegration)
  .settings(
    name := "flint"
  )

lazy val flintCore = (project in file("flint-core"))
  .settings(
    name := "flint-core",
    scalaVersion := scala212
  )

lazy val flintSparkIntegration = (project in file("flint-spark-integration"))
  .dependsOn(flintCore)
  .settings(
    name := "flint-spark-integration",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )

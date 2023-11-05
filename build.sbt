import Dependencies._

ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "trendyol.egitim"
ThisBuild / organizationName := "flink"

lazy val root = (project in file("."))
  .settings(
    name := "tflinkbase",
    libraryDependencies ++=Seq(
      flinkScala,
      flinkTableScala,
      flinkClients,
      flinkRuntimeWeb
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

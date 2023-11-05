import sbt._

object Dependencies {
  lazy val flinkVersion = "1.18.0"
  lazy val flinkScala = "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
  lazy val flinkTableScala = "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion
  lazy val flinkClients = "org.apache.flink" % "flink-clients" % flinkVersion
  lazy val flinkRuntimeWeb = "org.apache.flink" % "flink-runtime-web" % flinkVersion
}

name := "edith-impressions"
version := "0.1.0"
scalaVersion := "2.12.20"

val flinkVersion = "2.3.0"

libraryDependencies ++= Seq(
  // Provided by the Flink cluster at runtime.
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % Provided,
  "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
  // Bundled into the job jar.
  "org.apache.flink" % "flink-connector-kafka" % "4.0.1-2.0",
  "redis.clients" % "jedis" % "5.2.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2"
)

assembly / mainClass := Some("app.marlware.edith.ImpressionJob")
assembly / assemblyJarName := "edith-impressions.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}

ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "flink-exercises-scala-es"

version := "1.0.0"

organization := "com.flink"

ThisBuild / scalaVersion := "2.12.8"

val flinkVersion = "1.11.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-cep-scala" % flinkVersion,
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-planner-blink" % flinkVersion  % "provided" excludeAll (
    ExclusionRule("org.codehaus.janino", "janino"),
    ExclusionRule("org.apache.calcite.avatica", "avatica-core")
  ),
  "org.apache.flink" % "flink-table-common" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-wikiedits" % flinkVersion
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

//assembly / mainClass := Some("com.flink.examples.Job")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)

test in assembly := {}
assemblyMergeStrategy in assembly := {
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case "application.conf"                            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

commands += Command.command("assemblyWithProvidedDependencies") { state =>
  s"""set assemblyJarName in assembly := "${name.value}-assembly-with-provided-${version.value}.jar" """ ::
  """set assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true)""" ::
  """set assembly / fullClasspath := (Compile / fullClasspath).value""" :: "assembly" :: state
}
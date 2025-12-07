// Minimal plugins.sbt - keep only necessary plugins and a plugin resolver

// resolver for sbt plugin releases
resolvers += "sbt-plugin-releases" at "https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/"

// sbt-assembly plugin (for building fat/uber JAR)
// Using the widely-used coordinate (works with sbt 1.x)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")

name := "SentimentAnalysisScala"

version := "1.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.0.0" withSources()

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.1.0" withSources()

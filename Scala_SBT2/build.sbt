name := "Scala_SBT2"

version := "0.1"

scalaVersion := "2.11.8"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"

//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.1"
libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"

/*
libraryDependencies ++= {
  val sparkVer = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources()
  )
}
*/
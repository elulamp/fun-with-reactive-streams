name := "fun-with-reactive-streams"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
    "com.typesafe.akka" % "akka-stream-experimental_2.11" % "0.11",
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
    "io.reactivex" % "rxjava-reactive-streams" % "0.3.0"
)
    
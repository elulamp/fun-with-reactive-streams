name := "fun-with-reactive-streams"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
    "com.typesafe.akka" % "akka-stream-experimental_2.11" % "0.11",
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
    "junit" % "junit" % "4.11" % "test",
    "io.reactivex" % "rxjava-reactive-streams" % "0.3.0",
    "org.projectreactor" % "reactor-core" % "2.0.0.M1"
)

resolvers += "SpringSource snapshot" at "http://repo.spring.io/libs-milestone"


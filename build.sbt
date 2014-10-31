import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

seq(assemblySettings: _*)

name := "CallsVolDegreeStats"

version := "0.1"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

exportJars :=true

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"




// License: Apache 2.0
libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.4" % "it,test"


parallelExecution in Test := false

javaOptions in Test += "-XX:MaxPermSize=1G -XX:MaxMetaspaceSize=1G"

fork in Test := true



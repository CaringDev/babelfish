package ch.fhnw.imvs.babelfish.dsl

import ch.fhnw.imvs.babelfish.InternalConfiguration
import ch.fhnw.imvs.babelfish.dsl.core.Tr
import ch.fhnw.imvs.babelfish.util.Logging
import scala.tools.nsc.Settings

/** Settings to use for the Scala Interpreter */
class InterpreterSettings(config: InternalConfiguration) extends Settings with Logging {

  // set the correct class loader
  embeddedDefaults[Tr.type]

  // calculate the classpath, so that we can use pre-compiled stuff in the interpreter without problems
  classpath.value = getClass.getClassLoader match {
    //we only support URL Classloaders for now
    case cl: java.net.URLClassLoader => (cl.getURLs.map(_.getFile) :+ config.configurationFile).distinct.mkString(java.io.File.pathSeparator)
    case cl                          => logger.error(s"$cl is not a URLClassLoader"); ""
  }
}
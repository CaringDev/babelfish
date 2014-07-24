package ch.fhnw.imvs.babelfish.util

import java.io.File

object FileUtil {
  /** Returns a file handle pointing to the resource identified by 'name'.
    *
    * @param name the name of the resource
    * @return a file handle identified by 'name'
    */
  def getResourceAsFile(name: String): File = {
    val res = getClass.getResource(name)
    if (res == null) throw new IllegalArgumentException(s"Could not find resource $name")
    new File(res.toURI)
  }
}


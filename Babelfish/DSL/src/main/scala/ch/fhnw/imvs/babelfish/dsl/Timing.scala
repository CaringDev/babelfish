package ch.fhnw.imvs.babelfish.dsl

/** A time measurement for one DSL phase. */
case class Timing(name: String, millis: Long)

object Timing {
  val compile = "Compile"
  val execute = "Execute"
  val extract = "Extract"
  val sql = "SQL"
  val present = "Present"
}


package ch.fhnw.imvs.babelfish.schema

import ch.fhnw.imvs.babelfish.util.Logging

object SchemaValidator extends Logging {
  /** Validates the given schema.
    *
    * @param schema the schema to be validated
    * @return true if the schema is valid, false otherwise
    */
  def check(schema: Schema): Boolean = {
    var ok = true
    def error(s: String) {
      ok = false
      logger.error(s)
      println(s)
    }

    schema.nodes.foreach { n =>
      val nodeIndex = schema.nodes.indexOf(n)
      n.id.collect{ case e: SchemaEdge => e }.foreach { e =>
        if (e.from != n) error(s"ID $e of $n must not start on ${e.from}.")
        if (e.toCard != Cardinality.One) error(s"ID $e must have toCard == One.")
        if (e.from.equals(e.to)) error(s"ID $e must not be reflexive.")
        if (e.properties.nonEmpty) error(s"ID $e must not have properties.")
        if (nodeIndex <= schema.nodes.indexOf(e.to)) error(s"Node list must be topologically sorted (${e.to} must come after $n due to id $e)")
      }
      if (n.id.isEmpty) error(s"ID of $n must not be empty.")
    }

    ok
  }
}

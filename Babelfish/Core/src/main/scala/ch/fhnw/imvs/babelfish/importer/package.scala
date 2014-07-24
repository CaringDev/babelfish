package ch.fhnw.imvs.babelfish

import ch.fhnw.imvs.babelfish.schema.SchemaElement

/** Provides classes to deal with importing data into Babelfish. */
package object importer {
  type NamedSchemaProperties = Seq[(String, SchemaElement#SchemaProperty[_])]
}

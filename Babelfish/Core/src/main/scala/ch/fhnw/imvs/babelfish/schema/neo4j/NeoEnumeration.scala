package ch.fhnw.imvs.babelfish.schema.neo4j

import ch.fhnw.imvs.babelfish.schema.neo4j.NeoType._

class NeoEnumeration extends Enumeration {
  implicit val enumNeoTypeClass = EnumNeoType[this.type](this)
}

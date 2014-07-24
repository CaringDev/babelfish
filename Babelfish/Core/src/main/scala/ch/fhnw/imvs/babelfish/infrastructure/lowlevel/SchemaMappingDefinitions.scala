package ch.fhnw.imvs.babelfish.infrastructure.lowlevel

import org.neo4j.graphdb.DynamicRelationshipType

private[lowlevel] object SchemaMappingDefinitions {

  //use ' to preventing name conflicts with properties and EdgeNames <-- ''name is not accepted by compiler
  val META_PREFIX = "'"
  val ID_INDEX_PREFIX = "ID_INDEX_"
  val PROPERTY_EDGE_TYPE = DynamicRelationshipType.withName(META_PREFIX + "properties")
  val VERSION_RANGE_PROPERTY_KEY = META_PREFIX + "VERSION_RANGE"
}

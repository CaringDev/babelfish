package ch.fhnw.imvs.babelfish.infrastructure

package object lowlevel {

  import scala.language.implicitConversions

  private[lowlevel]type NeoNode = org.neo4j.graphdb.Node
  private[lowlevel]type NeoEdge = org.neo4j.graphdb.Relationship
  private[lowlevel]type NeoPropertyContainer = org.neo4j.graphdb.PropertyContainer
  private[lowlevel]type NeoDb = org.neo4j.graphdb.GraphDatabaseService
  private[lowlevel]type NeoDirection = org.neo4j.graphdb.Direction
}

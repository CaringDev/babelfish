package ch.fhnw.imvs.babelfish.schema

import ch.fhnw.imvs.babelfish.schema.neo4j.NeoType
import ch.fhnw.imvs.babelfish.util.{ RoseTree, Inner, Leaf }

/** Common base class for a schema. A schema describes the structure of the data graph.
  * It consists of [[ch.fhnw.imvs.babelfish.schema.SchemaNode]]s and [[ch.fhnw.imvs.babelfish.schema.SchemaEdge]]s.
  *
  * @param name the name of this schema
  * @param desc the description of this schema
  */
abstract class Schema(val name: String, val desc: String = "") {
  /** The collection of all available nodes in the schema. */
  def nodes: Seq[SchemaNode]
  /** The collection of all available edges in the schema. */
  def edges: Seq[SchemaEdge]

  /** Returns the schema node identified by 'name'.
    *
    * @param name The name of the schema node to be returned
    * @return The node if it can be found
    */
  def node(name: String): Option[SchemaNode] = nodeMap.get(name)

  /** Returns the schema edge identified by 'name'.
    *
    * @param name The name of the schema edge to be returned
    * @return The edge if it can be found
    */
  def edge(name: String): Option[SchemaEdge] = edgeMap.get(name)

  // name to node mapping for efficiency reasons
  private lazy val nodeMap = nodes.map(n => n.name -> n).toMap

  // name to edge mapping for efficiency reasons
  private lazy val edgeMap = edges.map(e => e.name -> e).toMap

  override def toString = s"Schema[$name](nodes:$nodes, edges:$edges)"

  require(SchemaValidator.check(this), "Illegal schema (see log)")
}

/** Common base trait for [[ch.fhnw.imvs.babelfish.schema.SchemaEdge]] and [[ch.fhnw.imvs.babelfish.schema.SchemaNode]]. */
trait SchemaElement { elem =>

  /** A property of either a SchemaNode or a SchemaEdge.
    *
    * There needs to be an [[ch.fhnw.imvs.babelfish.schema.neo4j.NeoType]][T] instance
    * available in order to create an SchemaProperty instance.
    *
    * SchemaProperty is consciously modelled as an inner class to benefit from Scala's
    * path dependent typing when accessing properties.
    *
    * @param desc the description of the property
    * @tparam T the type of the values for a property.
    */
  class SchemaProperty[T: NeoType](val desc: String = "") extends Identity {
    /** The parent / owner of this schema property. */
    val parent: SchemaElement = elem

    /** The name of this schema property.
      * It is computed from the class name to avoid duplication in a schema definition.
      */
    final lazy val name = getClass.getName.split("\\$").last

    /** The name of how it is stored in the database. */
    def dbName = s"${elem.name}#$name"

    override def toString = s"(Prop ${parent.name}.$name)"

    /** The NeoType instance of this property. */
    val neoType: NeoType[T] = implicitly[NeoType[T]]

    /** States whether this property is an ID property (invariant over time). */
    lazy val isId: Boolean = elem match {
      case sn: SchemaNode => sn.id.contains(this)
      case _              => false
    }
  }

  /** All properties of this schema element. */
  def properties: Seq[SchemaProperty[_]]

  /** All non-ID properties of this schema element. */
  lazy val dataProperties = properties.filterNot(_.isId)

  /** A description of this schema element. */
  def desc: String

  /** The name of this schema element.
    * It is computed from the class name to avoid duplication in a schema definition.
    */
  final lazy val name = getClass.getName.split("\\$").last
}

/** A trait to mark SchemaEdges or SchemaProperties as part of the identity of a SchemaNode. */
sealed trait Identity {
  /** States whether this is part of the identity of a SchemaNode. */
  def isId: Boolean
}

/** SchemaEdges statically describe the structure of the edges in the graph.
  * Edges are directed and point from -> to. The default cardinality is
  * many-to-one.
  */
abstract class SchemaEdge extends SchemaElement with Identity {
  /** The type of the source node. */
  type From <: SchemaNode
  /** The type of the target node. */
  type To <: SchemaNode
  /** The source node. */
  def from: From
  /** The target node. */
  def to: To
  /** The from-cardinality of this edge. */
  def fromCard: Cardinality = Cardinality.*
  /** The to-cardinality of this edge. */
  def toCard: Cardinality = Cardinality.One
  /** States whether this edge is part of the id of its source node. */
  lazy val isId: Boolean = from.id.contains(this)

  override def toString = s"(Edge $name[${from.name}->${to.name}])"
}

/** SchemaNodes statically describe the structure of the nodes in the graph. */
abstract class SchemaNode extends SchemaElement {
  override def toString: String = s"(Node $name)"
  /** A collection of all direct elements which form the identity of this schema node. */
  def id: Seq[Identity]

  /** A sequence of trees which describe the recursive structure of the elements which
    * form the identity of this schema node.
    *
    * Since edges may be part of the identity of a node and edges point to nodes
    * (which have identity properties and edges as well), the idProperties are modelled as
    * a sequence of trees with SchemaProperties at the leafs and SchemaEdges (the path to the properties)
    * in the inner tree nodes.
    *
    * Example:
    * Node1 {
    * id = {P1, Edge1}
    * }
    *
    * Edge1 {
    * from: Node1 to: Node2
    * }
    *
    * Node2 {
    * id = P2
    * }
    *
    * The idProperties of
    * Node1 are: Seq(Leaf(P1), Inner(Edge1,Leaf(P2)))
    * Node2 are: Seq(Leaf(P2))
    */
  def idProperties: Seq[RoseTree[SchemaEdge, SchemaElement#SchemaProperty[Any]]] =
    id.map { i =>
      (i: @unchecked) match {
        case p: SchemaProperty[Any] => Leaf(p)
        case e: SchemaEdge          => Inner(e, e.to.idProperties)
      }
    }
}

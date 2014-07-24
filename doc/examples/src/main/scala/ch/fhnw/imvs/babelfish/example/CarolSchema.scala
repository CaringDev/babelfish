package ch.fhnw.imvs.babelfish.example

import ch.fhnw.imvs.babelfish.schema._
import ch.fhnw.imvs.babelfish.schema.neo4j.NeoEnumeration
import org.joda.time.DateTime

object CarolSchema {

  object PetKind extends NeoEnumeration {
    val Dog = Value("D")
    val Cat = Value("C")
  }

  object carolsWorld extends Schema(
    "Carol's World", "Example Schema for Babelfish") {

    object Person extends SchemaNode {
      def desc = """Carol and her friends"""
      object Name extends SchemaProperty[String]("Call me this")
      object Birthday extends SchemaProperty[DateTime]("Better than 'Age'")

      def id = Seq(Name)
      val properties = Seq(Name, Birthday)
    }

    object Pet extends SchemaNode {
      def desc = """Dogs and Cats"""
      object Name extends SchemaProperty[Long]
      object Kind extends SchemaProperty[PetKind.Value]

      def id = Seq(Name, _owner_)
      val properties = Seq(Name)
    }

    object _loves_ extends SchemaEdge {
      type From = Person.type; type To = Person.type
      def from = Person; def to = Person

      def properties = Seq()
      def desc: String = "Lonesome but singular"
    }

    object _likes_ extends SchemaEdge {
      type From = Person.type; type To = Person.type
      def from = Person; def to = Person

      object HowMuch extends SchemaProperty[Int]
      def properties = Seq(HowMuch)
      def desc: String = "As you like it"

      override def toCard = Cardinality.*
    }

    object _owner_ extends SchemaEdge {
      type From = Pet.type; type To = Person.type
      def from = Pet; def to = Person

      def properties = Seq()
      def desc: String = "No pet without owner, at most one pet per owner."

      override def fromCard = Cardinality.?
    }

    def nodes: Seq[SchemaNode] = Seq(Person, Pet)
    def edges: Seq[SchemaEdge] = Seq(_loves_, _likes_, _owner_)
  }
}
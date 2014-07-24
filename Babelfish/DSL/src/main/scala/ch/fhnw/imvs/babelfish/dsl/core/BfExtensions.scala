package ch.fhnw.imvs.babelfish.dsl.core

import ch.fhnw.imvs.babelfish.infrastructure.versioning.{ VersionRangeSet, Versioned }
import ch.fhnw.imvs.babelfish.infrastructure.{ QueryResultNode, QueryResultEdge }
import ch.fhnw.imvs.babelfish.schema.{ SchemaEdge, SchemaNode, SchemaElement }
import ch.fhnw.imvs.babelfish.schema.neo4j.{ NeoType, SimpleNeoType }
import scala.reflect.ClassTag

trait BfExtensions { self: TrailsPrimitives =>

  final implicit class OrderSyntax[I, O, A](t1: Tr[I, O, A]) {
    def ascending(implicit order: Ordering[A]): Tr[I, O, A] = Tr(env => i => t1(env)(i).sortBy(_._2)(order))
    def descending(implicit order: Ordering[A]): Tr[I, O, A] = Tr(env => i => t1(env)(i).sortBy(_._2)(order.reverse))
  }

  /** Extract and label versioned values of a given property
    * @param p the property to extract
    * @param prefix a prefix to prepend to the property's name (optional)
    * @param ex an (implicit) extractor
    * @tparam S the type of the property owner
    * @tparam T the type of the property value
    * @return the input traverser expanded with the extracted property values
    */
  def ex[S <: SchemaElement, T: ClassTag](p: S#SchemaProperty[T], prefix: String = "")(implicit ex: Extractable[Versioned[T]]): Tr[State[S], State[S], Versioned[T]] =
    get(p).as(prefix + p.name)

  /** Extracts all properties of a schema element.
    * @note due to erasure all values are stored as Versioned[Any]
    * @param prefix to be added to the property name
    * @tparam S the type of the [[ch.fhnw.imvs.babelfish.schema.SchemaElement]] to get the properties from
    * @return a traverser which yields the versioned property values
    */
  def exAll[S <: SchemaElement: ClassTag](prefix: String = ""): Tr[State[S], State[S], Versioned[_]] =
    getType[S].flatMap { se =>
      val props: Seq[S#SchemaProperty[Any]] = se.properties.map(_.asInstanceOf[S#SchemaProperty[Any]])
      props.map { p =>
        ex(p, prefix)(findCT(p.neoType), findEx(p.neoType))
      }.reduce(_ ~ _)
    }

  def deg[S <: SchemaNode](implicit ev: ClassTag[S]): Tr[State[S], State[S], Map[SchemaEdge, Int]] =
    Tr(env => i => {
      if (i.path.head.validRanges.versions.size > 1) throw new NotImplementedError("Can only obtain degrees for one version")
      val node = getType[S](ev)(env)(i).head._2
      val outgoing = env.db.schema.edges.filter(_.from.equals(node))
      val incoming = env.db.schema.edges.filter(_.to.equals(node))
      val outDegs = outgoing.map(e => (e, outE(e)(env)(i.asInstanceOf[State[SchemaEdge#From]]).map(_._2)))
      val inDegs = incoming.map(e => (e, inE(e)(env)(i.asInstanceOf[State[SchemaEdge#To]]).map(_._2)))
      val degs = outDegs ++ inDegs
      // If we want to have degrees for more than one VersionRange
      // we need to count overlapping VersionRanges
      val qreBySE = degs.groupBy(_._1).mapValues(_.map(_._2).flatten.size)
      Stream((i, qreBySE))
    })

  /** Creates a path for each distinct value of the head versioned */
  def subs[S, O, A](tr: Tr[S, O, Versioned[A]]): Tr[S, S, (VersionRangeSet, A)] =
    Tr(e => s => tr(e)(s).map(_._2.extract).flatMap(_.map{ r => (s, r) }))

  /** Get the singleton instance of the current schema element
    * @param ev type information about the schema element (implicit)
    * @tparam S the type of the schema element
    * @return a traverser containing the singleton instance of S
    */
  private def getType[S <: SchemaElement](implicit ev: ClassTag[S]): Tr[State[S], State[S], S] =
    Tr(env => i => {
      val name = ev.runtimeClass.getName.split("\\$").last
      val elem = i.path.head match {
        case _: QueryResultEdge => env.db.schema.edge(name).get
        case _: QueryResultNode => env.db.schema.node(name).get
      }
      Stream((i, elem.asInstanceOf[S]))
    })

  // get around erasure, another possible solution could be based on HLists,
  // i.e. have static type information on nodes about their properties.
  private def findEx(n: NeoType[_]): Extractable[Versioned[Any]] = {
    def findSimpleEx[T](s: SimpleNeoType[T]): SimpleExtractType[T] = s match {
      case NeoType.BigDecimalTypeDecl => Extractable.BigDecimalExtractor
      case NeoType.BooleanTypeDecl    => Extractable.BooleanExtractor
      case NeoType.ByteTypeDecl       => Extractable.ByteExtractor
      case NeoType.CharTypeDecl       => Extractable.CharExtractor
      case NeoType.DateTimeTypeDecl   => Extractable.DateTimeExtractor
      case NeoType.DoubleTypeDecl     => Extractable.DoubleExtractor
      case NeoType.EnumNeoType(_)     => Extractable.anyExtractor[Any]
      case NeoType.FloatTypeDecl      => Extractable.FloatExtractor
      case NeoType.IntTypeDecl        => Extractable.IntExtractor
      case NeoType.LongTypeDecl       => Extractable.LongExtractor
      case NeoType.ShortTypeDecl      => Extractable.ShortExtractor
      case NeoType.StringTypeDecl     => Extractable.StringExtractor
    }
    n match {
      case s: SimpleNeoType[Any]          => Extractable.versionedExtractor(findSimpleEx(s))
      case v: NeoType.VectorTypeDecl[Any] => Extractable.versionedExtractor(Extractable.vectorExtractor(findSimpleEx(v.inner)).asInstanceOf[Versionable[Any]])
      case o: NeoType.OptionTypeDecl[Any] => Extractable.versionedExtractor(Extractable.optionExtractor(findSimpleEx(o.inner)).asInstanceOf[Versionable[Any]])
    }
  }

  private def findCT(n: NeoType[_]): ClassTag[Any] = (n match {
    case s: SimpleNeoType[Any]          => s.typeTag
    case v: NeoType.VectorTypeDecl[Any] => scala.reflect.classTag[Vector[Any]]
    case o: NeoType.OptionTypeDecl[Any] => scala.reflect.classTag[Option[Any]]
  }).asInstanceOf[ClassTag[Any]]
}

package ch.fhnw.imvs.babelfish.dsl.table

import ch.fhnw.imvs.babelfish.dsl.core.Extractable._
import ch.fhnw.imvs.babelfish.dsl.core._
import ch.fhnw.imvs.babelfish.schema.neo4j.NeoType._
import ch.fhnw.imvs.babelfish.util.Logging
import java.sql.{ Timestamp, Types }
import org.joda.time.DateTime

object SqlTypeMapping extends Logging {

  /** Get the Scala type and possibly a conversion function for a given Java.sql.Types type. */
  val toScala = Map[Int, (SimpleExtractType[_], Option[PartialFunction[Any, Any]])](
    (Types.INTEGER, (IntExtractor, None)),
    (Types.FLOAT, (FloatExtractor, None)),
    (Types.BIGINT, (LongExtractor, None)),
    (Types.DECIMAL, (BigDecimalExtractor, Some({ case x: java.math.BigDecimal => BigDecimal(x) }))),
    (Types.NUMERIC, (BigDecimalExtractor, Some({ case x: java.math.BigDecimal => BigDecimal(x) }))),
    (Types.DOUBLE, (DoubleExtractor, None)),
    (Types.BOOLEAN, (BooleanExtractor, None)),
    (Types.VARCHAR, (StringExtractor, None)),
    (Types.TIMESTAMP, (DateTimeExtractor, None))).withDefault { t =>
      logger.warn(s"Missing sql -> scala mapping for sql type $t")
      (StringTypeDecl, Some({ case x: Any => x.toString }: PartialFunction[Any, Any]))
    }

  /** Get the SQL type (Enum, String and possibly a conversion function) for a given SimpleExtractType. */
  def toSql = Map[SimpleExtractType[_], (Int, String, Option[PartialFunction[Any, Any]])](
    (IntExtractor, (Types.INTEGER, "INTEGER", None)),
    (FloatExtractor, (Types.FLOAT, "FLOAT", None)),
    (LongExtractor, (Types.BIGINT, "BIGINT", None)),
    (DoubleExtractor, (Types.DOUBLE, "DOUBLE", None)),
    (BooleanExtractor, (Types.BOOLEAN, "BOOLEAN", None)),
    (StringExtractor, (Types.VARCHAR, "VARCHAR", None)),
    (DateTimeExtractor, (Types.TIMESTAMP, "TIMESTAMP",
      Some({ case dt: DateTime => new Timestamp(dt.getMillis) }))),
    (BigDecimalExtractor, (Types.DECIMAL, "DECIMAL", None))).withDefault{ t =>
      logger.trace(s"Missing scala -> sql mapping for ${t.name}")
      (Types.VARCHAR, "VARCHAR", Some((x: Any) => x.toString))
    }

  /** Create a SQL type header from a Versionable 'td'. */
  def toSqlTypeHeader(td: Versionable[_]): String = {
    td match {
      case v: VectorExtractType[_] => withLength(toSql(v.inner)._2) + " ARRAY"
      case o: OptionExtractType[_] => withLength(toSql(o.inner)._2) + " NULL"
      case s: SimpleExtractType[_] => withLength(toSql(s)._2) + " NULL"
    }
  }

  private def withLength(s: String): String =
    s match {
      case "VARCHAR" => "VARCHAR(1000)" // arbitrary. should be enough and doesn't really matter for smaller ones...
      case x         => x
    }
}

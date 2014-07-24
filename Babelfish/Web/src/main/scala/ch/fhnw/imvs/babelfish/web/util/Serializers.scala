package ch.fhnw.imvs.babelfish.web.util

import org.json4s.{ Formats, Serializer }
import org.joda.time.DateTime
import org.json4s.JsonAST.{ JString, JValue }
import org.json4s.reflect.TypeInfo

object DateTimeSerializer extends Serializer[DateTime] {
  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = { case d: DateTime => new JString(d.toString) }
  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), DateTime] = { case (_, value: JString) => new DateTime(value.s) }
}

object EnumSerializer extends Serializer[Enumeration#Value] {
  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = { case d: Enumeration#Value => new JString(d.toString) }
  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Enumeration#Value] = {
    case (_, value: JString) =>
      throw new UnsupportedOperationException(s"Cannot guess enum type for ${value.s}")
  }
}

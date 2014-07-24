package ch.fhnw.imvs.babelfish.web.util

import ch.fhnw.imvs.babelfish.web.DefaultJsonServlet

object MediaType extends DefaultJsonServlet {

  private final val utf8 = "; charset=\"utf-8\""

  final val TEXT = formats("txt") + utf8
  final val CSV = "text/csv" + utf8
  final val JSON = formats("json") + utf8
  final val PDF = formats("pdf")
  final val XML = formats("xml")
  final val XHTML = formats("xhtml")
  final val SVG = formats("svg")

  final val CONTENT_TYPE = "Content-Type"
}

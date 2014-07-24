package ch.fhnw.imvs.babelfish.web

import ch.fhnw.imvs.babelfish.web.util.GraphViz

/** Servlet that allows checking for server capabilities. */
object Capabilities extends DefaultJsonServlet {
  get("/dot"){ GraphViz.checkDotVersion }
}

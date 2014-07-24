<%@ page isErrorPage="true" contentType="text/html;charset=UTF-8" %>
<html>
  <head>
    <title>Babelfish w/o H2O</title>
  </head>
  <body>
    <h1>OOOPS, something's fishy</h1>
    <h2>The Babelfish shows his belly ;-(</h2>
    <%=exception.getMessage()%>
  </body>
</html>
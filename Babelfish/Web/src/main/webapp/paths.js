/*globals $ */

function formatPathLine(pathElements) {
  'use strict';
  return _.map(pathElements, function (id) {
    switch (elementCache[id].kind) {
      case NODE_KIND:
        var nodeText = buildNodeInfo(id);
// TODO: display variable naming
//        if (el.hasOwnProperty("variable")) {
//          nodeText = el.variable + ": " + nodeText
//        }
        return nodeText;
      case EDGE_KIND:
        var edgeText = buildEdgeInfo(id);
//        // TODO: display variable naming
//        if (el.hasOwnProperty("variable")) {
//          edgeText = (id < 0) ? edgeText+el.variable+"-" : "-"+el.variable+edgeText
//        }
        return edgeText;
      default:
        return "Error: Unknown kind of path element: " + e;
    }
  }).join(' ');
}

function formatPaths() {
  'use strict';
  var formattedLines = _.map(pathCache, function (ids) {
    return formatPathLine(ids);
  });
  return '<ul><li>' + formattedLines.join('<\/li><li>') + '<\/li><\/ul>';
}

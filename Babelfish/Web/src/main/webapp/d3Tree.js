function formatTrees() {
  "use strict";

  function buildTrees(paths) {

    function tree(rootnode, subtree) {
      var result = {};
      result.node = rootnode;
      result.children = subtree;
      return result;
    }

    var filtered = _.filter(paths, function (x) { return !_.isEmpty(x); });
    var grouped = _.groupBy(filtered, function (path) { return _.head(path).id; });
    return _.map(grouped, function (value, key) {
      return tree(
        _.head(_.head(value)),  // any head is a valid root node, so we take the first one
        buildTrees(_.map(value, function (val) { return _.rest(val); }))
      );
    });
  }

  function dummyNode() {
    return {
      kind    : NODE_KIND,
      id      : Math.floor(-Math.random() * 100000001), // long negative random number to create distinct dummy nodes
      idProperties : {},
      nodeType    : "(dummy node)"
    };
  }

  // only recompute tree data if the cache is empty:
  if (edgeCache.length !== 0 || treeDataCache.length !== 0) {

    var paddedElementPaths = _.map(pathCache, function(idpath) {
      var elementPath = _.map(idpath, function(id) { return elementCache[id]; });

      // add dummy nodes at the beginning and the end if necessary
      var headDangling = _.head(elementPath).kind === EDGE_KIND;
      if (headDangling) { elementPath.unshift(dummyNode()); }

      var tailDangling = _.last(elementPath).kind === EDGE_KIND;
      if (tailDangling) { elementPath.push(dummyNode()); }

      return elementPath;
    });

    // fill edgeCache
    _.forEach(paddedElementPaths, function (path) {
      _.forEach(path, function (el, idx, p) {
        if (el.kind === EDGE_KIND) {
          var parentNodeId = p[idx - 1].id;
          var childNodeId = p[idx + 1].id;
          var key = [parentNodeId, childNodeId];
          edgeCache[key] = el.id;
        }
      });
    });

    var nodePaths = _.map(paddedElementPaths, function(path) {
      return _.filter(path, function(el) { return el.kind === NODE_KIND; });
    });

    treeDataCache = buildTrees(nodePaths)
  }

  _.map(treeDataCache, function (treedata) {
    createTreeSvg(treedata);
  });
}


function nodeText(nodeContainer) {
  "use strict";
  var node = nodeContainer.node, idProps = [], prop;
  for (prop in node.idProperties) {
    if (node.idProperties.hasOwnProperty(prop)) {
      idProps.push(node.idProperties[prop]);
    }
  }
  return node.nodeType + '(' + idProps + ')';
}


function createTreeSvg(treedata) {
  "use strict";
  // generate svg:

  var width = 960, height = 1100;

  var svg = d3.select("#queryResult").append("svg")
      .attr("width", width)
      .attr("height", height)
      .append("g")
      .attr("transform", "translate(40,0)");


  // build tree:

  var tree = d3.layout.tree()
      .sort(function (a, b) { return nodeText(a) > nodeText(b); })
      .size([height, width - 160]);

  var nodes = tree.nodes(treedata), links = tree.links(nodes);

  // load and paint nodes, with active circles and descriptive text:

  var node = svg.selectAll(".node")
      .data(nodes)
      .enter().append("g")
      .attr("class", "node")
      .attr("onclick", function (d) { return "showNodeInfo(" + d.node.id + "); return false;"; })
      .attr("transform", function (d) { return "translate(" + d.y + "," + d.x + ")"; });

  node.append("circle")
      .attr("r", 4.5);

  node.append("text")
      .attr("dy", "-.75em")
      .attr("text-anchor", function (d) { return "middle"; })
      .text(nodeText);


  // load and paint edges:

  var diagonal = d3.svg.diagonal()
      .projection(function (d) { return [d.y, d.x]; });

  var link = svg.selectAll(".link")
      .data(links)
      .enter().append("path")
      .attr("class", "link")
      .attr("d", diagonal)
      .attr("onclick", function (d) { return "showEdgeInfo(" + edgeCache[[d.source.node.id, d.target.node.id]] + "); return false;"; });

  link.append("text")
      .attr("x", function (d) { return (d.source.x + d.target.x) / 2; })
      .attr("y", function (d) { return (d.source.y + d.target.y) / 2; })
      .attr("text-anchor", "middle")
      .text(function (d) { return edgeCache[[d.source.node.id, d.target.node.id]]; });  // should display edge id, but doesn't work, it seems...
}
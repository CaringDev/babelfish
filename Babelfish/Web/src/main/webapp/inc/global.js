/*globals $,Storage,localStorage,setTimeout,document*/
function setup() {
  'use strict';
  $('#header').html(
    '<h1>Babelfish Web Viewer<\/h1>' +
      '<nav>' +
      '<a href="index.html">Query<\/a> ' +
      '<a href="schema.html">Schema<\/a> ' +
      '<a href="jobs.html">Jobs<\/a> ' +
      '<a href="log.html">Log<\/a> ' +
      '<a href="status.html">Status<\/a> ' +
      '<\/nav><hr\/>'
  );
  $.ajaxSetup({
    cache: false,
    xhrFields: { withCredentials: true },
    crossDomain: true
  });
  // busy cursor during ajax calls
  $(document).ajaxStart(function () {
    $("html").addClass('busy');
  }).ajaxStop(function () {
    $("html").removeClass('busy');
  });
  // nice title attributes
  $(document).tooltip();
}

function timeify() {
  'use strict';
  $('#content').find('td.time').text(function (idx, old) {return (new Date(Number(old))).toLocaleString(); });
  $('#content').find('td.timeDiff').text(function (idx, old) {
    var split = old.split('-'), from = Number(split[0]), to = Number(split[1]);
    var date = new Date(to - from), h = date.getUTCHours(), m = date.getUTCMinutes() + (h * 60), s = date.getUTCSeconds(), ms = date.getUTCMilliseconds();
    if (m < 10) { m = "0" + m; }
    if (s < 10) { s = "0" + s; }
    return m + ':' + s + '.' + ms;
  });
}

/**
 * Sorts an array of json objects by some common property, or sub-property.
 * @param {array} objArray
 * @param {array|string} prop Dot-delimited string or array of (sub)properties
 */
function sortJsonArrayByProp(objArray, prop){
    if (arguments.length<2){
        throw new Error("sortJsonArrayByProp requires 2 arguments");
    }
    if (objArray && objArray.constructor===Array){
        var propPath = (prop.constructor===Array) ? prop : prop.split(".");
        objArray.sort(function(a,b){
            for (var p in propPath){
                if (typeof(a[propPath[p]])!="undefined" && typeof(b[propPath[p]])!="undefined"){
                    a = a[propPath[p]];
                    b = b[propPath[p]];
                }
            }
            // convert numeric strings to integers	    
            a = String(a).match(/^\d+$/) ? +a : a;
            b = String(b).match(/^\d+$/) ? +b : b;
            return ( (a < b) ? -1 : ((a > b) ? 1 : 0) );
        });
    }
}

function getLocalValue(key) {
  'use strict';
  if (Storage !== "undefined") {
    return JSON.parse(localStorage.getItem(key));
  }
}

function setLocalValue(key, value) {
  'use strict';
  if (Storage !== "undefined") {
    localStorage.setItem(key, JSON.stringify(value));
  }
}

function downloadDialog(id) {
  'use strict';

  function formatLinks(format) {
    var aggregated = ['false', 'true'];
    function aggText(aggr) { if (aggr === 'true') { return '(aggr.)'; } else { return ''; } }
    var layout = ['dot', 'sfdp'];
    var res = '';
    aggregated.forEach(function (a) {
      layout.forEach(function (l) {
        res +=  ' <a href="bf/jobs/results/' + id + '.' + format + '?layout=' + l + '&aggregated=' + a + '">' + l + aggText(a) + '<\/a> ';
      });
      res += '<br/>';
    });
    return '<td>' + res + '<\/td>';
  }

  function links() {
    var formats = ['pdf', 'svg'];
    var res = '';
    formats.forEach(function (f) {
      res += formatLinks(f);
    });
    return res;
  }

  var dialog = '<div title="Download result ' + id + '">' +
    '<table><tr><th><img src="inc/images/PDF.svg" height="20px"/><\/th><th><img src="inc/images/SVG.svg" height="20px"/><\/th><th>GV<\/th><\/tr>' +
    '<tr>' + links() +
    '<td><a href="bf/jobs/results/' + id + '.gv?aggregated=false">GraphViz<\/a><br/>' +
    '<a href="bf/jobs/results/' + id + '.gv?aggregated=true">GraphViz (aggr.)<\/a><\/td><\/tr><\/table>' +
    '<a href="bf/jobs/results/' + id + '.graphml">GraphML<\/a>' +
    '<\/div>';
  $(dialog).dialog({autoOpen: true, modal: true, height: 'auto', width: 'auto', resizable: false, closeText: '', close: function () { $(this).dialog('destroy').remove(); }});
}

function refresh(cont) {
  'use strict';
  if ($('#autoRefresh').prop('checked')) {
    setTimeout(function () { refresh(cont); }, 5000);
    cont();
  }
}

/*globals $ */
(function () {
  'use strict';
  $.views.helpers('getFields',
    function (object) {
      var key, value,
        fieldsArray = [];
      for (key in object) {
        if (object.hasOwnProperty(key)) {
          value = object[key];
          // For each property/field add an object to the array, with key and value
          fieldsArray.push({
            key: key,
            value: value
          });
        }
      }
      // Return the array, to be rendered using {{for ~getFields(object)}}
      return fieldsArray;
    });
  $.templates({
    propertyTemplate:
      '{{if properties.length !== 0}}<table>' +
      '{{for properties}}<tr title="{{>description}}"><td>' +
      '{{if isId}}<em>{{/if}}' +
      '{{>name}}' +
      '{{if isId}}</em>{{/if}}' +
      '</td><td>{{>propertyType}}</td></tr>{{/for}}' +
      '</table>{{/if}}',
    schemaTemplate:
      '<h2>Nodes</h2>' +
      '{{for nodes}}' +
        '<p style="margin-top:15px;margin-bottom:2px;"><strong title="{{if description}}{{>description}}{{/if}}">{{>name}}</strong></p>' +
        '{{for #data tmpl="propertyTemplate" /}}' +
      '{{/for}}' +
      '<h2>Edges</h2>' +
      '{{for edges}}' +
        '<p><strong title="{{if description}}{{>description}}{{/if}}">{{>name}}</strong> ' +
        '<em>{{>fromSchemaNode}} &rarr; {{>toSchemaNode}}</em></p>' +
        '{{for #data tmpl="propertyTemplate" /}}' +
      '{{/for}}',
    jobTemplate:
      '<table id="jobTable">' + '<thead><th>Query</th><th>Duration / Start</th><th></th><th></th></thead>' + '{{for #data}}' +
      '<tr id="job{{>id}}">' +
      '<td class="query">{{>id}}#{{>query}}</td>' +
      '{{if finished}}' +
        '<td class="timeDiff">{{>startedAt}}-{{>finishedAt}}</td>' +
        '<td>{{if resultType === "Path"}}' +
          '<input type="button" value="Download" onclick="$(\'#autoRefresh\').prop(\'checked\', false); downloadDialog({{>id}});" />' +
        '{{else if resultType === "Table"}}' +
          '<a href="bf/jobs/results/{{>id}}.csv"><img height="20px" src="inc/images/CSV.png"/></a>' +
        '{{/if}}</td>' +
        '<td><a href="#" onclick="cancelJob({{>id}})">Delete</a></td>' +
      '{{else}}' +
        '<td class="time">{{>startedAt}}</td><td></td>' +
        '<td><a href="#" onclick="cancelJob({{>id}})">Cancel</a></td>' +
      '{{/if}}' +
      '</tr>' +
      '{{/for}}' + '</table>',
    failedJobTemplate:
      '<table id="failedJobTable">' + '<thead><th>Query</th><th>Duration</th><th>Delete</th></thead>' + '{{for #data}}' +
        '<tr id="job{{>id}}">' +
          '<td>{{>query}}</td>' +
          '<td class="timeDiff">{{>startedAt}}-{{>finishedAt}}</td>' +
          '<td><a href="#" onclick="cancelJob({{>id}})">Delete</a></td>' +
        '</tr>' +
      '{{/for}}' + '</table>',
    nodeTemplate:
      '<strong>{{>nodeType}}</strong> ({{>nodeId}})<br/>' +
        '{{for ~getFields(idProperties)}}<table><tr>' +
          '<td colspan="2"><em class="attrKey">{{>key}}</em></td><td>{{>value}}</td>' +
        '</tr></table>{{/for}}<table>' +
        '{{for ~getFields(properties)}}' +
          '<tr><td colspan="3" class="attrKey">{{>key}}</td></tr>' +
          '{{for value}}<tr>' +
            '<td>{{>version.from}}</td><td> - </td><td>{{>version.to}}</td><td>{{>value}}</td>' +
          '</tr>{{/for}}' +
        '{{/for}}' +
      '</table>',
    edgeTemplate:
      '<strong>{{>relationshipType}}</strong> ({{>edgeId}})<br/>' +
      'from node {{>fromNode}} to node {{>toNode}}<br/>' +
      'valid in Ranges:<br>' +
      '<table>' +
        '{{for validRanges}}' +
          '<tr><td>{{>from}}</td><td>to</td><td>{{>to}}</td></tr>' +
        '{{/for}}' +
      '</table><table>' +
        '{{for ~getFields(properties)}}' +
        '<tr><td>{{>key}}</td><td>{{for value}}{{>value}}{{/for}}</td></tr>' +
        '{{/for}}' +
      '</table>',
    logTemplate:
      '<table>' +
        '<thead><tr><th>When</th><th>Level</th><th>Logger</th><th>Message</th></tr></thead>' +
        '{{for #data}}' +
          '<tr class="{{>level}}"><td class="time">{{>timestamp}}</td><td>{{>level}}</td><td>{{>logger}}</td><td>{{>message}}</td></tr>' +
        '{{/for}}' +
      '</table>',
    tableTemplate:
      '<hr><table id="queryResultTable">' +
        '<thead><tr>{{for columnNames}}<th>{{>#data}}</th>{{/for}}</tr></thead>' +
        '{{for rows}}<tr>{{for #data}}<td>{{>#data}}</td>{{/for}}</tr>{{/for}}' +
      '</table>',
    statusTemplate:
      '{{for #data}}' +
        '<h2>VM stats</h2>' +
          'Babelfish version: {{>bfVersion}}, Memory used / total / max: {{>usedMemoryGB.toFixed(2)}} / {{>totalMemoryGB.toFixed(2)}} / {{>maxMemoryGB.toFixed(2)}} GB' +
      '{{/for}}',
    statisticsTemplate:
      '{{for #data}}' +
        'DbVersion: {{>dbVersion}}, ConfigurationVersion {{>configVersion}}, BF version used to build configuration: {{>configBFversion}}, configuration jar: {{>configJarPath}}</p>' +
        '<h2>Nodes</h2>' +
        '<table id="nodesTable">' +
          '{{for ~getFields(numberOfNodes)}}' +
            '<tr><td>{{>key}}</td><td>{{>value}}</td></tr>' +
          '{{/for}}' +
        '</table>' +
        '<h2>Edges</h2>' +
        '<table id="edgesTable">' +
          '<tbody>{{for ~getFields(numberOfEdges)}}' +
            '<tr><td>{{>key}}</td><td>{{>value}}</td></tr>' +
          '{{/for}}</tbody>' +
        '</table>' +
      '{{/for}}',
    timingsTemplate: '<p><small>Timings: {{for timings}}{{>name}}: {{>millis}}ms {{/for}}</small></p>'
  });
}());

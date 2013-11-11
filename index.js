/*jshint node:true*/
"use strict";

var sql = require("nodemssql"),
    stream = require("stream");

function query(connectionString,sqlCmd) {
  var fields,_meta,_row,ret;
  var out = stream.PassThrough({objectMode:true});
  var q = sql.query(connectionString,sqlCmd);

  function push(d) {
    _row = d;
    if (ret) out.push(ret);
    ret = {};
  }

  q.once('meta',function(d) {
    _meta = d;
    fields = d.map(function(d) {
      return d.name;
    });
  });

  q.on('row',push);

  q.on('column',function(i,d) {
    ret.__proto__ = {_meta:_meta,_row:_row};
    ret[fields[i]] = d;
  });

  q.on('done',function() {
    push();
    out.end();
  });

  q.on('error',function(e) {
    console.log('error',e);
    out.emit('error',e);
  });

  return out;
}

module.exports = query;
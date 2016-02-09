"use strict";

var impala = require('../');

var client = impala.createClient({
  host: '192.168.93.128'
});

var sql = 'SELECT * FROM sample_07 LIMIT 5';

client.query(sql, function (error, result) {
    console.log(result);
});

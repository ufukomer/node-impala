"use strict";

var impala = require('../');

var client = impala.createClient({
    host: '192.168.93.128'
});

var sql = "SELECT * FROM sample_07 limit 5";

client.query(sql)
    .then(function (result) {
        console.log(result);
    })
    .catch(function (err) {
        console.error(err);
    });

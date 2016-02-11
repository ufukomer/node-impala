"use strict";

var impala = require('../');

var client = impala.createClient({
    host: '192.168.93.128',
    mapping: false
});

var sql = "SELECT * FROM sample_07 LIMIT 5";

client.query(sql, function (err, result) {
    if (err) {
        console.error(err);
    } else {
        console.log(result);
    }
});

"use strict";

var impala = require('../');
var beeswax_types = require('../lib/chd5-2.1_5.3.0/beeswax_types')

var client = impala.createClient({
    host: '192.168.140.131'
});

var query = new beeswax_types.Query({
    query: 'select * from sample_07 LIMIT 5'
});

client.query(query);
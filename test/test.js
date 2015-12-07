"use strict";

var thrift = require('thrift');
var ImpalaService = require('../lib/chd5-2.1_5.3.0/ImpalaService');
var beeswax_types = require('../lib/chd5-2.1_5.3.0/beeswax_types');

var connection = thrift.createConnection('192.168.140.131', 21000, {
    transport: thrift.TBufferedTransport,
    protocol: thrift.TBinaryProtocol,
    timeout: 60000
});

connection.on('error', function (err) {
    console.error(err);
});

var query = new beeswax_types.Query({
    query: 'SELECT * FROM sample_07 LIMIT 5'
})

var client = thrift.createClient(ImpalaService, connection);

client.query(query, function (err, handle) {
    if (err) console.error(err)
    else {
        client.get_state(handle, function (err, state) {
            if (err) console.error(err)
            else {
                client.explain(query, function (err, explain) {
                    if (err) console.error(err)
                    else {
                        client.fetch(handle, false, 100, function (err, result) {
                            if (err) console.error(err)
                            else {
                                client.get_results_metadata(handle, function (err, metaData) {
                                    if (err) console.error(err)
                                    else {
                                        console.log(explain.textual);
                                        console.log(metaData.schema.fieldSchemas);
                                        console.log(result.data);
                                        if (state === beeswax_types.QueryState.FINISHED)
                                            connection.end();
                                    }
                                });
                            }
                        })
                    }
                });
            }
        })
    }
});